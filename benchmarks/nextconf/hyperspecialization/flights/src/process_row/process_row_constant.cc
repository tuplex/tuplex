#include "./process_row.h"
#include "./util.h"

void ProcessRow(int64_t day_of_month, int64_t day_of_week, const std::string &fl_date,
                const std::string &origin_city_name, const std::string &dest_city_name,
                double actual_elapsed_time, int64_t _year, int64_t _quarter, int64_t _month,
                const std::string &_op_unique_carrier, int64_t _crs_dep_time, int64_t _crs_arr_time,
                double _cancelled, const std::string &_cancellation_code, double _diverted,
                const std::string &_div_reached_dest, const std::string &_div_actual_elapsed_time,
                char output_data[], size_t &output_size) {
  // constants
  const int64_t year = 2019;
  const int64_t quarter = 1;
  const int64_t month = 1;
  const std::string op_unique_carrier("9E");
  const int64_t crs_dep_time = 1645;
  const int64_t crs_arr_time = 1732;
  const double cancelled = 0.0;
  const std::string cancellation_code;
  const double diverted = 0.0;
  const std::string div_reached_dest;
  const std::string div_actual_elapsed_time;

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
                            "%ld,%ld,%ld,%ld,%ld,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%d,%s,%f\n", year,
                            quarter, month, day_of_month, day_of_week, fl_date.c_str(),
                            op_unique_carrier.c_str(), origin_city.c_str(), origin_state.c_str(),
                            dest_city.c_str(), dest_state.c_str(), crs_arr_time_fmt.c_str(),
                            crs_dep_time_fmt.c_str(), cancelled_bool, cancellation_code_clean,
                            diverted_bool, cancellation_reason, actual_elapsed_time_calc);
  } catch (...) {
  }
}
