#ifndef PROCESS_ROW_NARROW_H
#define PROCESS_ROW_NARROW_H

#include <string>

extern "C" {
void ProcessRow(uint8_t day_of_month, uint8_t day_of_week, const std::string &fl_date,
                const std::string &origin_city_name, const std::string &dest_city_name,
                double actual_elapsed_time, uint16_t year, uint8_t quarter, uint8_t month,
                const std::string &op_unique_carrier, uint16_t crs_dep_time, uint16_t crs_arr_time,
                double cancelled, const std::string &cancellation_code, double diverted,
                const std::string &div_reached_dest, const std::string &div_actual_elapsed_time,
                char output_data[], size_t &output_size);
}

#endif  // PROCESS_ROW_NARROW_H
