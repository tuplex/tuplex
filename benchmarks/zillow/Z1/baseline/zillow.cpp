#define FMT_HEADER_ONLY
#include "./csvmonkey.h"
#include "./fmt/include/fmt/format.h"

#include <glob.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

static std::vector<std::string> input_files;
static char const *output_path;

// implicit selection pushdown
struct InputData {
  std::string title;
  std::string address;
  std::string city;
  std::string state;
  std::string postal_code;
  std::string price;
  std::string facts_and_features;
  std::string url;

  InputData(std::string &&title, std::string &&address, std::string &&city,
            std::string &&state, std::string &&postal_code, std::string &&price,
            std::string &&facts_and_features, std::string &&url)
      : title(std::move(title)),
        address(std::move(address)),
        city(std::move(city)),
        state(std::move(state)),
        postal_code(std::move(postal_code)),
        price(std::move(price)),
        facts_and_features(std::move(facts_and_features)),
        url(std::move(url)) {}
};
static const int NUM_INPUT_ROWS = 48740299;

static int output_size = 0;
static const int OUTPUT_DATA_SIZE = 200'000'000;
static char output_data[OUTPUT_DATA_SIZE];  // 200 MB
static bool preload;

// static const int NUM_COLUMNS = 7;
// enum COLUMN {
// TITLE = 0,
// ADDRESS,
// CITY,
// STATE,
// POSTAL_CODE,
// PRICE,
// FACTS_AND_FEATURES,
// URL
//};

// Timestamp helpers
static std::map<std::string, std::chrono::nanoseconds> timestamps;
static void Timestamp(
    const char *const name,
    const std::chrono::time_point<std::chrono::high_resolution_clock> &start,
    const std::chrono::time_point<std::chrono::high_resolution_clock> &end) {
  if (name == std::string("total"))
    throw std::runtime_error("Reserved name 'total' for timestamp");

  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  std::cout << name << " stage: " << duration.count() << " ns" << std::endl;
  timestamps[name] = duration;
}

static void DumpTimestamps() {
  std::chrono::nanoseconds total(0);
  std::cout << "{";
  for (auto it = timestamps.begin(); it != timestamps.end(); ++it) {
    if (it != timestamps.begin()) std::cout << ",";
    std::cout << "\"" << it->first << "\":" << it->second.count();
    total += it->second;
  }
  if (timestamps.size() > 0) std::cout << ",";
  std::cout << "\"total\":" << total.count() << "}";
  std::cout << std::endl;
}

// Argument parsing
static void ParseArguments(int argc, char **argv) {
  // validate input
  if (!(argc == 5 || argc == 6))
    throw std::runtime_error(
        "Usage: ./zillow --path <input file path> --output_path <output file "
        "path> [--preload]");
  if (strcmp(argv[1], "--path") != 0)
    throw std::runtime_error("Invalid first flag: " + std::string(argv[1]));
  if (strcmp(argv[3], "--output_path") != 0)
    throw std::runtime_error("Invalid second flag: " + std::string(argv[3]));
  if (argc == 6 && strcmp(argv[5], "--preload") != 0)
    throw std::runtime_error("Invalid third flag: " + std::string(argv[5]));

  // set output path
  output_path = argv[4];
  // set preload flag
  preload = argc == 6;

  // glob input files
  glob_t glob_result;
  memset(&glob_result, 0, sizeof(glob_result));

  int r;
  if ((r = glob(argv[2], GLOB_TILDE, nullptr, &glob_result)) != 0) {
    globfree(&glob_result);
    throw std::runtime_error("glob() failed with return: " + std::to_string(r));
  }

  input_files.clear();
  input_files.reserve(glob_result.gl_pathc);
  for (size_t i = 0; i < glob_result.gl_pathc; i++) {
    input_files.emplace_back(glob_result.gl_pathv[i]);
  }

  globfree(&glob_result);
}

static inline size_t CaseInsensitiveSearch(const std::string &subject,
                                           const char *const term,
                                           size_t term_size) {
  auto it = std::search(
      subject.begin(), subject.end(), term, term + term_size,
      [](char c1, char c2) { return std::tolower(c1) == std::tolower(c2); });
  return it - subject.begin();
}

static inline void ToLowercase(const std::string &from, std::string &to) {
  // this to lower function might be buggy
  to.resize(from.size());
  std::transform(from.begin(), from.end(), to.begin(),
                 [](unsigned char c) { return std::tolower(c); });
}

static inline void RemoveCharacterInPlace(std::string &s, char c) {
  s.erase(std::remove(s.begin(), s.end(), c), s.end());
}

// Compute Path
template <bool clean>
static inline int64_t ExtractFromFactsAndFeatures(
    const std::string &facts_and_features, const char *ss1,
    __attribute__((unused)) size_t ss1_len, const char *ss2, size_t ss2_len) {
  size_t max_idx, split_idx;
  max_idx = facts_and_features.find(ss1);
  if (max_idx == std::string::npos) max_idx = facts_and_features.length();
  split_idx = facts_and_features.rfind(ss2, max_idx - ss2_len);
  if (split_idx == std::string::npos)
    split_idx = 0;
  else
    split_idx += ss2_len + 1;
  auto sub = facts_and_features.substr(split_idx, max_idx - split_idx);
  if (clean) RemoveCharacterInPlace(sub, ',');
  return std::stoll(sub);
}

static inline void ProcessRow(const std::string &title,
                              const std::string &address,
                              const std::string &city, const std::string &state,
                              const std::string &postal_code,
                              const std::string &price,
                              const std::string &facts_and_features,
                              const std::string &url) {
  try {  // need to wrap in try/catch to ignore all the badly-formatted rows
         // (e.g. those that don't have integers in the correct places in facts
         // and features, etc.)
    std::string temp;

    // extractBd
    auto bedrooms = ExtractFromFactsAndFeatures<false>(facts_and_features,
                                                       " bd", 3, ",", 1);
    if (!(bedrooms < 10ll)) return;  // filter

    // extractType
    if (CaseInsensitiveSearch(title, "house", 5) == title.length())
      return;  // filter
    std::string type = "house";

    // zipcode
    auto zipcode = fmt::format("{:05d}", std::stoi(postal_code));

    // city
    std::string cleaned_city;
    ToLowercase(city, cleaned_city);
    cleaned_city[0] = std::toupper(city[0]);

    // extractBa
    auto bathrooms = ExtractFromFactsAndFeatures<false>(facts_and_features,
                                                        " ba", 3, ",", 1);

    // extractSqft
    auto sqft = ExtractFromFactsAndFeatures<true>(facts_and_features, " sqft",
                                                  5, "ba ,", 4);

    // extractOffer
    std::string offer;
    if (CaseInsensitiveSearch(title, "sale", 4) != title.length())
      offer = "sale";
    else if (CaseInsensitiveSearch(title, "rent", 4) != title.length())
      offer = "rent";
    else if (CaseInsensitiveSearch(title, "sold", 4) != title.length())
      offer = "sold";
    else if (CaseInsensitiveSearch(title, "foreclose", 9) != title.length())
      offer = "foreclosed";
    else
      ToLowercase(title, offer);

    // extractPrice
    int64_t cleaned_price = 0;
    if (offer == "sold") {
      auto t1 = facts_and_features.find("Price/sqft:") + 11 + 1;
      auto t2 = facts_and_features.find('$', t1) + 1;
      auto t3 = facts_and_features.find(", ", t1) - 1;
      auto price_per_sqft = std::stoll(facts_and_features.substr(t2, t3 - t2));
      cleaned_price = price_per_sqft * sqft;
    } else if (offer == "rent") {
      temp = price.substr(1, price.rfind('/') - 1);
      RemoveCharacterInPlace(temp, ',');
      cleaned_price = std::stoll(temp);
    } else {
      temp = price.substr(1);
      RemoveCharacterInPlace(temp, ',');
      cleaned_price = std::stoll(temp);
    }
    if (!(cleaned_price > 100000ll && cleaned_price < 20000000ll))
      return;  // filter

    // build output string
    output_size += snprintf(
        output_data + output_size, OUTPUT_DATA_SIZE - output_size,
        "%s,%s,%s,%s,%s,%ld,%ld,%ld,%s,%s,%ld\n", url.c_str(), zipcode.c_str(),
        address.c_str(), cleaned_city.c_str(), state.c_str(), bedrooms,
        bathrooms, sqft, offer.c_str(), type.c_str(), cleaned_price);
  } catch (...) {
  }
}

static inline void InitializeHeader() {
  output_size = snprintf(
      output_data, 73,
      "url,zipcode,address,city,state,bedrooms,bathrooms,sqft,offer,type,"
      "price\n");
}

int main(int argc, char **argv) {
  // parse input
  ParseArguments(argc, argv);
  std::cout << input_files.size() << " input files found" << std::endl;

  // TODO: does having runtime_error in the timed code, instead of
  // fprintf/return 1, have runtime implications?

  /*
   * 2. Process Data
   */
  csvmonkey::CsvCell *title, *address, *city, *state, *postal_code, *price,
      *facts_and_features, *url;
  std::vector<csvmonkey::FieldPair> fields = {
      {"title", &title},
      {"address", &address},
      {"city", &city},
      {"state", &state},
      {"postal_code", &postal_code},
      {"price", &price},
      {"facts and features", &facts_and_features},
      {"url", &url}};

  if (preload) {
    // load the input files into memory
    auto start_io = std::chrono::high_resolution_clock::now();

    InitializeHeader();
    // TODO: might be better with parallel vectors for some
    // auto-vectorizations, need to benchmark
    auto input_data_buf = malloc(NUM_INPUT_ROWS * sizeof(InputData));
    auto input_data = static_cast<InputData *>(input_data_buf);
    size_t num_input_rows = 0;

    for (const auto &input_file : input_files) {
      auto fd = open(input_file.c_str(), O_RDONLY);
      csvmonkey::FdStreamCursor stream(fd);
      csvmonkey::CsvReader<csvmonkey::FdStreamCursor> reader(stream);

      // get the relevant columns
      // csvmonkey::CsvCursor &row = reader.row();
      if (!reader.read_row())
        throw std::runtime_error("Cannot read header row");
      reader.extract_fields(fields);

      // save the columns
      while (reader.read_row()) {
        new (input_data + num_input_rows)
            InputData(title->as_str(), address->as_str(), city->as_str(),
                      state->as_str(), postal_code->as_str(), price->as_str(),
                      facts_and_features->as_str(), url->as_str());
        num_input_rows++;
      }
      close(fd);
    }
    auto end_io = std::chrono::high_resolution_clock::now();
    Timestamp("load", start_io, end_io);

    // perform the row processing
    auto start_compute = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < num_input_rows; i++) {
      ProcessRow(input_data[i].title, input_data[i].address, input_data[i].city,
                 input_data[i].state, input_data[i].postal_code,
                 input_data[i].price, input_data[i].facts_and_features,
                 input_data[i].url);
    }

    auto end_compute = std::chrono::high_resolution_clock::now();
    Timestamp("compute", start_compute, end_compute);
  } else {
    auto start_transform = std::chrono::high_resolution_clock::now();
    InitializeHeader();
    for (const auto &input_file : input_files) {
      auto fd = open(input_file.c_str(), O_RDONLY);
      csvmonkey::FdStreamCursor stream(fd);
      csvmonkey::CsvReader<csvmonkey::FdStreamCursor> reader(stream);

      // get the relevant columns
      // csvmonkey::CsvCursor &row = reader.row();
      if (!reader.read_row())
        throw std::runtime_error("Cannot read header row");
      reader.extract_fields(fields);

      // immediately process the row
      while (reader.read_row()) {
        ProcessRow(title->as_str(), address->as_str(), city->as_str(),
                   state->as_str(), postal_code->as_str(), price->as_str(),
                   facts_and_features->as_str(), url->as_str());
      }
      close(fd);
    }
    auto end_transform = std::chrono::high_resolution_clock::now();
    Timestamp("transform", start_transform, end_transform);
  }

  /*
   * 3. Output to file
   */
  // create output directory if it doesn't exist
  struct stat st;
  if (!(stat(output_path, &st) == 0 && S_ISDIR(st.st_mode))) {  // doesn't exist
    if (mkdir(output_path, 0755) == -1)  // failed to create
      throw std::runtime_error("Failed to create output directory");
  }

  auto start_output = std::chrono::high_resolution_clock::now();

  // open output file
  auto ofile_name = std::string(output_path) + "/" + std::string("part0.csv");
  auto ofile = open(ofile_name.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0664);

  // dump output
  write(ofile, output_data, output_size);
  close(ofile);
  auto end_output = std::chrono::high_resolution_clock::now();
  Timestamp("output", start_output, end_output);

  /*
   * 4. Dump timestamps
   */
  DumpTimestamps();
}
