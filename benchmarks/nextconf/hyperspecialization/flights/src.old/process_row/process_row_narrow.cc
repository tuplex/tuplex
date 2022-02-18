#include "./process_row_narrow.h"
#include "./util.h"

void ProcessRow(uint8_t day_of_month, uint8_t day_of_week, const std::string &fl_date,
                const std::string &origin_city_name, const std::string &dest_city_name,
                double actual_elapsed_time, uint16_t year, uint8_t quarter, uint8_t month,
                const std::string &op_unique_carrier, uint16_t crs_dep_time, uint16_t crs_arr_time,
                double cancelled, const std::string &cancellation_code, double diverted,
                const std::string &div_reached_dest, const std::string &div_actual_elapsed_time,
                char output_data[], size_t &output_size) {
  try {  // need to wrap in try/catch to ignore all the badly-formatted rows
         // (e.g. those that don't have integers in the correct places in facts
         // and features, etc.)
    auto origin_city = ExtractCity(origin_city_name);
    auto origin_state = ExtractState(origin_city_name);
    auto dest_city = ExtractCity(dest_city_name);
    auto dest_state = ExtractState(dest_city_name);
    auto crs_arr_time_fmt = FormatTime(crs_arr_time);
    auto crs_dep_time_fmt = FormatTime(crs_dep_time);
    auto cancellation_code_clean = CleanCode(cancellation_code);
    auto diverted_bool = diverted > 0;
    auto cancelled_bool = cancelled > 0;
    auto cancellation_reason = DivertedCode(diverted_bool, cancellation_code_clean);
    auto actual_elapsed_time_calc =
        FillInTimes(actual_elapsed_time, div_reached_dest, div_actual_elapsed_time);

    // build output string
    output_size += snprintf(output_data + output_size, OUTPUT_DATA_SIZE - output_size,
                            "%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%d,%s,%f\n", year,
                            quarter, month, day_of_month, day_of_week, fl_date.c_str(),
                            op_unique_carrier.c_str(), origin_city.c_str(), origin_state.c_str(),
                            dest_city.c_str(), dest_state.c_str(), crs_arr_time_fmt.c_str(),
                            crs_dep_time_fmt.c_str(), cancelled_bool, cancellation_code_clean,
                            diverted_bool, cancellation_reason, actual_elapsed_time_calc);
  } catch (...) {
  }
}
