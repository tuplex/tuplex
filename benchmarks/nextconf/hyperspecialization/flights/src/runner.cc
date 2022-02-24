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
#include "lyra.h"
#include <sys/stat.h>

#define NUM_COLUMNS 110

// dynamic libraries
auto pr_orig = "./process_row/process_row_orig.so";
auto pr_constant = "./process_row/process_row_constant.so";
auto pr_narrow = "./process_row/process_row_narrow.so";
//

//// typedef the process row function
//int64_t (*write_callback)(void *userData, uint8_t *buf, size_t buf_size);

// functors, set to nullptr
// extern "C" int64_t init_aggregate();
// extern "C" int64_t process_cells(void *userData, char **cells, int64_t *cell_sizes);
// extern "C" int64_t fetch_aggregate(uint8_t** buf, size_t* buf_size);
int64_t
(*cell_functor_f)(void *userData, char **cells, int64_t *cell_sizes) = nullptr; // number of cells is known upfront! It's 110.

int64_t (*init_agg_f)(void *userData) = nullptr;
int64_t (*fetch_agg_f)(void *userData, uint8_t** buf, size_t* buf_size) = nullptr;


static std::vector<std::string> input_files;
std::string input_path;
std::string output_path;
std::string dl_path;

// static const int NUM_INPUT_ROWS = 48740299;

static size_t output_size = 0;
static const size_t OUTPUT_DATA_SIZE = 4'000'000'000;
static char *output_data = nullptr;  // [OUTPUT_DATA_SIZE];  // 4 GB
static bool preload = false;

int64_t write_callback(void *userData, uint8_t *buf, size_t buf_size) {
    if(!output_data) {
        output_data = new char[OUTPUT_DATA_SIZE];
    }
    if(output_size  + buf_size < OUTPUT_DATA_SIZE) {
        memcpy(output_data + output_size, buf, buf_size);
        output_size += buf_size;
        return 0;
    } else {
        return 1;
    }
}

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
        std::cout << "\"" << it->first << "\":" << it->second.count() / 1000000000.0;
        total += it->second;
    }
    if (timestamps.size() > 0) std::cout << ",";
    std::cout << "\"total\":" << total.count()  / 1000000000.0<< "}";
    std::cout << std::endl;
}

// Argument parsing
static int ParseArguments(int argc, char **argv) {
    bool show_help = false;

    // construct CLI
    auto cli = lyra::cli();
    cli.add_argument(lyra::help(show_help));
    cli.add_argument(lyra::opt(input_path, "input_path").name("--input-path").name("-i").help(
            "path where flights_on_time.csv is stored"));
    cli.add_argument(
            lyra::opt(output_path, "output_path").name("--output-path").name("-o").help("path where to store output"));
    cli.add_argument(lyra::opt(dl_path, "dl_path").name("--dl-path").name("-d").help(
            "path to shared lib holding process functor"));
    cli.add_argument(lyra::opt(preload).name("--preload").help("whether to preload CSV cells, and then operate on that array"));

    auto result = cli.parse({argc, argv});
    if (!result) {
        std::cerr << "Error parsing command line: " << result.errorMessage() << std::endl;
        return 1;
    }

    if (show_help) {
        std::cout << cli << std::endl;
        return 0;
    }

    // glob input files
    glob_t glob_result;
    memset(&glob_result, 0, sizeof(glob_result));

    int r;
    if ((r = glob(input_path.c_str(), GLOB_TILDE, nullptr, &glob_result)) != 0) {
        globfree(&glob_result);
        throw std::runtime_error("glob() failed with return: " + std::to_string(r));
    }

    input_files.clear();
    input_files.reserve(glob_result.gl_pathc);
    for (size_t i = 0; i < glob_result.gl_pathc; i++) {
        input_files.emplace_back(glob_result.gl_pathv[i]);
    }

    globfree(&glob_result);
    return 0;
}

static inline void InitializeHeader() {
    if(!init_agg_f) {
        output_size =
                snprintf(output_data, 201,
                         "Year,Quarter,Month,DayOfMonth,DayOfWeek,FlDate,OpUniqueCarrier,OriginCity,"
                         "OriginState,DestCity,DestState,CrsArrTime,CrsDepTime,Cancelled,CancellationCode,"
                         "Diverted,CancellationReason,ActualElapsedTime\n");
    }
    else {
        output_size =
                snprintf(output_data, 512,
                         "day,month,avg_weather_delay,std_weather_delay\n");
    }

}


// function to preload the data (i.e., parse all cells and then hand them off)
std::vector<std::tuple<char**,int64_t*>> preloadCells(const std::string& path) {
    std::vector<std::tuple<char**,int64_t*>> v;

    int64_t row_number = 0;
    for (const auto &input_file: input_files) {
        auto fd = open(input_file.c_str(), O_RDONLY);
        csvmonkey::FdStreamCursor stream(fd);
        csvmonkey::CsvReader<csvmonkey::FdStreamCursor> reader(stream);

        // get the relevant columns
        // csvmonkey::CsvCursor &row = reader.row();
        if (!reader.read_row()) throw std::runtime_error("Cannot read header row");
        csvmonkey::CsvCell *year, *quarter, *month, *day_of_month, *day_of_week, *fl_date,
                *op_unique_carrier, *origin_city_name, *dest_city_name, *crs_dep_time, *crs_arr_time,
                *cancelled, *cancellation_code, *diverted, *actual_elapsed_time, *div_reached_dest,
                *div_actual_elapsed_time;
        std::vector<csvmonkey::FieldPair> fields = {
                {"YEAR",                    &year},
                {"QUARTER",                 &quarter},
                {"MONTH",                   &month},
                {"DAY_OF_MONTH",            &day_of_month},
                {"DAY_OF_WEEK",             &day_of_week},
                {"FL_DATE",                 &fl_date},
                {"OP_UNIQUE_CARRIER",       &op_unique_carrier},
                {"ORIGIN_CITY_NAME",        &origin_city_name},
                {"DEST_CITY_NAME",          &dest_city_name},
                {"CRS_DEP_TIME",            &crs_dep_time},
                {"CRS_ARR_TIME",            &crs_arr_time},
                {"CANCELLED",               &cancelled},
                {"CANCELLATION_CODE",       &cancellation_code},
                {"DIVERTED",                &diverted},
                {"ACTUAL_ELAPSED_TIME",     &actual_elapsed_time},
                {"DIV_REACHED_DEST",        &div_reached_dest},
                {"DIV_ACTUAL_ELAPSED_TIME", &div_actual_elapsed_time}};
        reader.extract_fields(fields);

        // immediately process the row
        while (reader.read_row()) {
            auto &row = reader.row();
            // use processCells Functor which gets specialized to whatever works for the file.
            if (NUM_COLUMNS == row.count) {
                char **cells = new char*[NUM_COLUMNS];
                int64_t *cell_sizes = new int64_t[NUM_COLUMNS];

                // generate cells array by pointing to memory and head it over to cells functor
                for (unsigned i = 0; i < 110; ++i) {
                    if (row.cells[i].ptr) {
                        auto cell = row.cells[i].as_str();
                        auto cell_size = cell.length() + 1;
                        cells[i] = (char*)malloc(cell_size);
                        memcpy(cells[i], cell.c_str(), cell_size);
                        cell_sizes[i] = cell_size;
                    } else {
                        cells[i] = (char*)malloc(1);
                        cells[i][0] = '\0';
                        cell_sizes[i] = 1;
                    }
                }

                // add to result vector
                v.emplace_back(cells, cell_sizes);
            } else {
                // failure...
                std::cerr << "row with wrong number of columns found" << std::endl;
            }
            row_number++;
        }

        close(fd);
    }

    std::cout<<"Preloaded "<<row_number<<" rows"<<std::endl;
    return v;
}

void freeCells(std::vector<std::tuple<char**,int64_t*>>& v) {
    for(auto& cell_infos : v) {
        for(unsigned i = 0; i < NUM_COLUMNS; ++i) {
            auto& cells = std::get<0>(cell_infos);
            auto& cell_sizes = std::get<1>(cell_infos);

            if(cells && cells[i]) {
                free(cells[i]);
                cells[i] = nullptr;
            }
            delete [] cells;
            cells = nullptr;
            delete [] cell_sizes;
            cell_sizes = nullptr;
        }
    }
    v.clear();
}

int main(int argc, char **argv) {
    // allocate output space
    output_data = new char[OUTPUT_DATA_SIZE];  // 4 GB

    // parse input
    int rc = ParseArguments(argc, argv);
    if (rc != 0)
        return rc;
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
            {"YEAR",                    &year},
            {"QUARTER",                 &quarter},
            {"MONTH",                   &month},
            {"DAY_OF_MONTH",            &day_of_month},
            {"DAY_OF_WEEK",             &day_of_week},
            {"FL_DATE",                 &fl_date},
            {"OP_UNIQUE_CARRIER",       &op_unique_carrier},
            {"ORIGIN_CITY_NAME",        &origin_city_name},
            {"DEST_CITY_NAME",          &dest_city_name},
            {"CRS_DEP_TIME",            &crs_dep_time},
            {"CRS_ARR_TIME",            &crs_arr_time},
            {"CANCELLED",               &cancelled},
            {"CANCELLATION_CODE",       &cancellation_code},
            {"DIVERTED",                &diverted},
            {"ACTUAL_ELAPSED_TIME",     &actual_elapsed_time},
            {"DIV_REACHED_DEST",        &div_reached_dest},
            {"DIV_ACTUAL_ELAPSED_TIME", &div_actual_elapsed_time}};

    // ROW PROCESSING FUNCTIONS ------
    // 1. make sure we're not optimizing for the calling convention by making both versions of the
    // function have the same number of parameters.
    // 2. also, split [ProcessRow] out to a different library, compile and load this function at
    // runtime in this driver program
    void *handle;
    char *error;
    std::cout << "SO Path: " << dl_path << std::endl;
    handle = dlopen(dl_path.c_str(), RTLD_LAZY);
    if (!handle) {
        fprintf(stderr, "%s\n", dlerror());
        exit(1);
    }
    dlerror();

    // retrieve func pointers from shared object
    *(void **) (&cell_functor_f) = dlsym(handle, "process_cells");
    if ((error = dlerror()) != nullptr) {
        fprintf(stderr, "%s\n", error);
        exit(1);
    }
    *(void **) (&init_agg_f) = dlsym(handle, "init_aggregate");
    *(void **) (&fetch_agg_f) = dlsym(handle, "fetch_aggregate");



    auto start_transform = std::chrono::high_resolution_clock::now();

    if(init_agg_f)
        init_agg_f(nullptr);

    InitializeHeader();

    std::vector<std::tuple<char**,int64_t*>> preloadedCells;
    size_t num_parsed_rows = 0;
    // preload, or not?
    if(preload) {
        std::cout<<"Preloading data..."<<std::endl;
        if(input_files.size() == 1) {
            preloadedCells = preloadCells(input_files.front());
        } else {
            for (const auto &input_file: input_files) {
                auto vp = preloadCells(input_file);
                std::copy(vp.begin(), vp.end(), std::back_inserter(preloadedCells));
                vp.clear();
            }
        }
        num_parsed_rows = preloadedCells.size();
        Timestamp("preload", start_transform, std::chrono::high_resolution_clock::now());

        // transform
        start_transform = std::chrono::high_resolution_clock::now();

        // basically call cell-functor on each element!
        int64_t row_number = 0;
        for(auto cell_infos : preloadedCells) {
            auto& cells = std::get<0>(cell_infos);
            auto& cell_sizes = std::get<1>(cell_infos);

            rc = cell_functor_f(reinterpret_cast<void*>(write_callback), cells, cell_sizes);

            if (0 != rc) {
                std::cerr << "processing row " << row_number << " failed." << std::endl;
                break;
            }
            row_number++;
        }

        // free cells... --> done after parse!

    } else {
        int64_t row_number = 0;
        for (const auto &input_file: input_files) {
            auto fd = open(input_file.c_str(), O_RDONLY);
            csvmonkey::FdStreamCursor stream(fd);
            csvmonkey::CsvReader<csvmonkey::FdStreamCursor> reader(stream);

            // get the relevant columns
            // csvmonkey::CsvCursor &row = reader.row();
            if (!reader.read_row()) throw std::runtime_error("Cannot read header row");
            reader.extract_fields(fields);

            const char *empty_str = "";
            char **cells = new char *[NUM_COLUMNS];
            char **cell_mem = new char *[NUM_COLUMNS];
            size_t* cell_mem_size = new size_t[NUM_COLUMNS];
            int64_t cell_sizes[NUM_COLUMNS];
            for (unsigned i = 0; i < 110; ++i) {
                cells[i] = nullptr;
                cell_sizes[i] = 0;
                cell_mem[i] = (char*)malloc(1024);
                cell_mem_size[i] = 1024;
            }

            // immediately process the row
            while (reader.read_row()) {
                auto &row = reader.row();
                // use processCells Functor which gets specialized to whatever works for the file.
                if (NUM_COLUMNS == row.count) {
                    // generate cells array by pointing to memory and head it over to cells functor
                    for (unsigned i = 0; i < 110; ++i) {
                        if (row.cells[i].ptr) {
                            auto cell = row.cells[i].as_str();
                            cell_sizes[i] = cell.length() + 1;

                            // alloc cells! => free then again.
                            // note the +1 for the zero-terminate character.
                            if(cell_mem_size[i] < cell.length() + 1) {
                                cell_mem_size[i] = cell.length() + 1;
                                cell_mem[i] = static_cast<char *>(realloc(cell_mem[i], cell_mem_size[i]));
                            }

                            cells[i] = cell_mem[i];
                            memcpy(cells[i], cell.c_str(), cell.length() + 1);
                        } else {
                            cells[i] = const_cast<char *>(empty_str);
                        }
                    }

                    // call functor
                    rc = cell_functor_f(reinterpret_cast<void*>(write_callback), cells, cell_sizes);

                    if (0 != rc)
                        std::cerr << "processing row " << row_number << " failed." << std::endl;
                } else {
                    // failure...
                    std::cerr << "row with wrong number of columns found" << std::endl;
                }
                row_number++;
            }

            // free cells
            for (unsigned i = 0; i < NUM_COLUMNS; ++i)
                free(cell_mem[i]);

            close(fd);

            num_parsed_rows += row_number;
        }
    }

    // fetch aggregate?
    if(fetch_agg_f) {
        uint8_t *buf = nullptr;
        size_t buf_size = 0;

        uint64_t num_specialized_rows = 0;
        num_specialized_rows = num_parsed_rows;
        fetch_agg_f(&num_specialized_rows, &buf, &buf_size);
        write_callback(nullptr, buf, buf_size);
        free(buf);
    }

    auto end_transform = std::chrono::high_resolution_clock::now();
    freeCells(preloadedCells);
    Timestamp("transform", start_transform, end_transform);
    // unload lib
    dlclose(handle);

    /*
     * 3. Output to file
     */
    // create output directory if it doesn't exist
    struct stat st;
    if (!(stat(output_path.c_str(), &st) == 0 && S_ISDIR(st.st_mode))) {  // doesn't exist
        if (mkdir(output_path.c_str(), 0755) == -1)                         // failed to create
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
