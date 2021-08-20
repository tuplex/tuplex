//#define FMT_HEADER_ONLY
//#define SPECIALIZE_CONSTANTS
#define PR_ORIG 0
#define PR_CONSTANT 1
#define PR_VERSION PR_ORIG

#include "./csvmonkey.h"

#include <glob.h>
#include <dlfcn.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

// dynamic libraries
auto pr_orig = "./process_row/process_row_orig.so";
auto pr_constant = "./process_row/process_row_constant.so";

#if PR_VERSION == PR_ORIG
auto pr_path = pr_orig;
#elif PR_VERSION == PR_CONSTANT
auto pr_path = pr_constant;
#endif
// ----

static std::vector<std::string> input_files;
static char const *output_path;

// static const int NUM_INPUT_ROWS = 48740299;

static size_t output_size = 0;
static const size_t OUTPUT_DATA_SIZE = 4'000'000'000;
static char *output_data;  // [OUTPUT_DATA_SIZE];  // 4 GB
static bool preload;

// Timestamp helpers
static std::map<std::string, std::chrono::nanoseconds> timestamps;
static void Timestamp(const char *const name,
                      const std::chrono::time_point<std::chrono::high_resolution_clock> &start,
                      const std::chrono::time_point<std::chrono::high_resolution_clock> &end) {
  if (name == std::string("total")) throw std::runtime_error("Reserved name 'total' for timestamp");

  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
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
        "Usage: ./basic --path <input file path> --output_path <output file "
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

static inline void InitializeHeader() {
  output_size =
      snprintf(output_data, 201,
               "Year,Quarter,Month,DayOfMonth,DayOfWeek,FlDate,OpUniqueCarrier,OriginCity,"
               "OriginState,DestCity,DestState,CrsArrTime,CrsDepTime,Cancelled,CancellationCode,"
               "Diverted,CancellationReason,ActualElapsedTime\n");
}

int main(int argc, char **argv) {
  // allocate output space
  output_data = new char[OUTPUT_DATA_SIZE];  // 4 GB

  // parse input
  ParseArguments(argc, argv);
  std::cout << input_files.size() << " input files found" << std::endl;

  // TODO: does having runtime_error in the timed code, instead of
  // fprintf/return 1, have runtime implications?

  /*
   * 2. Process Data
   */
  csvmonkey::CsvCell *year, *quarter, *month, *day_of_month, *day_of_week, *fl_date,
      *op_unique_carrier, *origin_city_name, *dest_city_name, *crs_dep_time, *crs_arr_time,
      *cancelled, *cancellation_code, *diverted, *actual_elapsed_time, *div_reached_dest,
      *div_actual_elapsed_time;

  std::vector<csvmonkey::FieldPair> fields = {
      {"YEAR", &year},
      {"QUARTER", &quarter},
      {"MONTH", &month},
      {"DAY_OF_MONTH", &day_of_month},
      {"DAY_OF_WEEK", &day_of_week},
      {"FL_DATE", &fl_date},
      {"OP_UNIQUE_CARRIER", &op_unique_carrier},
      {"ORIGIN_CITY_NAME", &origin_city_name},
      {"DEST_CITY_NAME", &dest_city_name},
      {"CRS_DEP_TIME", &crs_dep_time},
      {"CRS_ARR_TIME", &crs_arr_time},
      {"CANCELLED", &cancelled},
      {"CANCELLATION_CODE", &cancellation_code},
      {"DIVERTED", &diverted},
      {"ACTUAL_ELAPSED_TIME", &actual_elapsed_time},
      {"DIV_REACHED_DEST", &div_reached_dest},
      {"DIV_ACTUAL_ELAPSED_TIME", &div_actual_elapsed_time}};

  // ROW PROCESSING FUNCTIONS ------
  // 1. make sure we're not optimizing for the calling convention by making both versions of the
  // function have the same number of parameters.
  // 2. also, split [ProcessRow] out to a different library, compile and load this function at
  // runtime in this driver program
  void *handle;
  void (*ProcessRow)(int64_t, int64_t, const std::string &, const std::string &,
                     const std::string &, double, int64_t, int64_t, int64_t, const std::string &,
                     int64_t, int64_t, double, const std::string &, double, const std::string &,
                     const std::string &, char[], size_t &);
  char *error;
  std::cout << "SO Path: " << pr_path << std::endl;
  handle = dlopen(pr_path, RTLD_LAZY);
  if (!handle) {
    fprintf(stderr, "%s\n", dlerror());
    exit(1);
  }
  dlerror();
  *(void **)(&ProcessRow) = dlsym(handle, "ProcessRow");
  if ((error = dlerror()) != nullptr) {
    fprintf(stderr, "%s\n", error);
    exit(1);
  }

  if (preload) {
    throw std::runtime_error("Not implemented");
  } else {
    auto start_transform = std::chrono::high_resolution_clock::now();
    InitializeHeader();
    for (const auto &input_file : input_files) {
      auto fd = open(input_file.c_str(), O_RDONLY);
      csvmonkey::FdStreamCursor stream(fd);
      csvmonkey::CsvReader<csvmonkey::FdStreamCursor> reader(stream);

      // get the relevant columns
      // csvmonkey::CsvCursor &row = reader.row();
      if (!reader.read_row()) throw std::runtime_error("Cannot read header row");
      reader.extract_fields(fields);

      // immediately process the row
      while (reader.read_row()) {
        // TODO: override csvmonkey to get other data types out for the range specialization idea
        ProcessRow(
            day_of_month->as_double(), day_of_week->as_double(), fl_date->as_str(),
            origin_city_name->as_str(), dest_city_name->as_str(), actual_elapsed_time->as_double()
#if (PR_VERSION == PR_ORIG)
                                                                      ,
            year->as_double(), quarter->as_double(), month->as_double(),
            op_unique_carrier->as_str(), crs_dep_time->as_double(), crs_arr_time->as_double(),
            cancelled->as_double(), cancellation_code->as_str(), diverted->as_double(),
            div_reached_dest->as_str(), div_actual_elapsed_time->as_str()
#elif (PR_VERSION == PR_CONSTANT)
                                                                      ,
            0, 0, 0, "", 0, 0, 0, "", 0, "", ""
#endif
                                            ,
            output_data, output_size);
      }
      close(fd);
    }
    auto end_transform = std::chrono::high_resolution_clock::now();
    Timestamp("transform", start_transform, end_transform);
  }
  dlclose(handle);

  /*
   * 3. Output to file
   */
  // create output directory if it doesn't exist
  struct stat st;
  if (!(stat(output_path, &st) == 0 && S_ISDIR(st.st_mode))) {  // doesn't exist
    if (mkdir(output_path, 0755) == -1)                         // failed to create
      throw std::runtime_error("Failed to create output directory");
  }

  auto start_output = std::chrono::high_resolution_clock::now();

  // open output file
  auto ofile_name = std::string(output_path) + "/" + std::string("part0.csv");
  auto ofile = open(ofile_name.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0664);

  // dump output
  auto r = write(ofile, output_data, output_size);
  if (r < 0) {
    throw std::runtime_error("write failed!");
  }
  close(ofile);
  auto end_output = std::chrono::high_resolution_clock::now();
  Timestamp("output", start_output, end_output);

  /*
   * 4. Dump timestamps
   */
  DumpTimestamps();
}
