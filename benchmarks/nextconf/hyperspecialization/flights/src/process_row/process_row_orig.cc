#include "./process_row.h"
#include "./util.h"

#define NUM_COLUMNS 110

//// this will be linked with the runner exec!
//int64_t (*write_callback)(void* userData, uint8_t* buf, size_t buf_size);

// new version, condensed closer to Tuplex codegen
extern "C" int64_t process_cells(void *userData, char **cells, int64_t *cell_sizes) {

//    try {
        // extract certain funcs
        auto year = std::stoll(cells[0]);
        auto quarter = std::stoll(cells[1]);
        auto month = std::stoll(cells[2]);
        auto dayofmonth = std::stoll(cells[3]);
        auto dayofweek = std::stoll(cells[4]);
        std::string fldate = cells[5];
        std::string opuniquecarrier = cells[6];
        std::string origincityname = cells[15];
        std::string destcityname = cells[24];

        std::string depdelay = cells[31];
        std::string taxiout = cells[36];
        std::string taxiin = cells[39];

        std::string arrdelay = cells[42];

        std::string cancellationcode = cells[48];

        std::string crselapsedtime = cells[50];

        std::string airtime = cells[52];
        std::string distance = cells[54];
        std::string carrierdelay = cells[56];
        std::string weatherdelay = cells[57];
        std::string nasdelay = cells[58];
        std::string securitydelay = cells[59];
        std::string lateaircraftdelay = cells[60];
        std::string divreacheddest = cells[65];
        std::string divactualelapsedtime = cells[66];


        auto crsdeptime = std::stoll(cells[29]);
        auto crsarrtime = std::stoll(cells[40]);
        auto cancelled = std::stod(cells[47]);
        auto diverted = std::stod(cells[49]);
        std::string actualelapsedtime = cells[51];

        auto origin_city = ExtractCity(origincityname);
        auto origin_state = ExtractState(origincityname);
        auto dest_city = ExtractCity(destcityname);
        auto dest_state = ExtractState(destcityname);
        auto crs_arr_time_fmt = FormatTime(crsarrtime);
        auto crs_dep_time_fmt = FormatTime(crsdeptime);
        auto cancellation_code_clean = CleanCode(cancellationcode);
        auto diverted_bool = diverted > 0;
        auto cancelled_bool = cancelled > 0;
        auto cancellation_reason = DivertedCode(diverted_bool, cancellation_code_clean);
        auto actual_elapsed_time_calc =
                FillInTimes(actualelapsedtime, divreacheddest, divactualelapsedtime);

        // build output string (csv)
        auto buf = (uint8_t*)malloc(4096); // 4K page, resize if it doesn't fit
        size_t required_bytes = snprintf(reinterpret_cast<char *>(buf), 4096,
                                         "%lld,%lld,%lld,%lld,%lld,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%d,%s,%s\n", year,
                                         quarter, month, dayofmonth, dayofweek, fldate.c_str(),
                                         opuniquecarrier.c_str(), origin_city.c_str(), origin_state.c_str(),
                                         dest_city.c_str(), dest_state.c_str(), crs_arr_time_fmt.c_str(),
                                         crs_dep_time_fmt.c_str(), cancelled_bool, cancellation_code_clean,
                                         diverted_bool, cancellation_reason, actual_elapsed_time_calc.c_str());
        if(required_bytes >= 4096) {
            buf = (uint8_t*)realloc(buf, required_bytes + 128);
            required_bytes = snprintf(reinterpret_cast<char *>(buf), 4096,
                                      "%lld,%lld,%lld,%lld,%lld,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%d,%s,%s\n", year,
                                      quarter, month, dayofmonth, dayofweek, fldate.c_str(),
                                      opuniquecarrier.c_str(), origin_city.c_str(), origin_state.c_str(),
                                      dest_city.c_str(), dest_state.c_str(), crs_arr_time_fmt.c_str(),
                                      crs_dep_time_fmt.c_str(), cancelled_bool, cancellation_code_clean,
                                      diverted_bool, cancellation_reason, actual_elapsed_time_calc.c_str());
        }

        auto callback = reinterpret_cast<int64_t(*)(void* userData, uint8_t* buf, size_t buf_size)>(userData);
        int rc = callback(userData, buf, required_bytes);
        free(buf);
        return rc;
//    } catch(...) {
//        return 1;
//    }
}