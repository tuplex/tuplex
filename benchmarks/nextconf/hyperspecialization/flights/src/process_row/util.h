#ifndef HYPERSPECIAL_FLIGHTS_UTIL_H
#define HYPERSPECIAL_FLIGHTS_UTIL_H

#define FMT_HEADER_ONLY
#include "./fmt/include/fmt/format.h"

static const size_t OUTPUT_DATA_SIZE = 4'000'000'000;
// UTILITY FUNCTIONS ----------------------
static inline const char *CleanCode(const std::string &cancellation_code) {
  if (cancellation_code == "A")
    return "carrier";
  else if (cancellation_code == "B")
    return "weather";
  else if (cancellation_code == "C")
    return "national air system";
  else if (cancellation_code == "D")
    return "security";
  else
    return "";
}

static inline const char *DivertedCode(bool diverted, const std::string &cancellation_code) {
  if (diverted)
    return "diverted";
  else if (!cancellation_code.empty())
    return cancellation_code.c_str();
  else
    return "";
}

static inline std::string FillInTimes(const std::string& actual_elapsed_time, const std::string &div_reached_dest,
                                 const std::string &div_actual_elapsed_time) {
  if (!div_reached_dest.empty()) {
    if (std::stod(div_reached_dest) > 0)
      return div_actual_elapsed_time;
    else
      return actual_elapsed_time;
  } else {
    return actual_elapsed_time;
  }
}

// --- https://stackoverflow.com/questions/216823/how-to-trim-a-stdstring -----
// trim from start (in place)
static inline void ltrim(std::string &s) {
  s.erase(s.begin(),
          std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
}

// trim from end (in place)
static inline void rtrim(std::string &s) {
  s.erase(
      std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); }).base(),
      s.end());
}

// trim from both ends (in place)
static inline void trim(std::string &s) {
  ltrim(s);
  rtrim(s);
}
// ----------------------------------------------------------------------------

static inline std::string ExtractCity(const std::string &city_and_state) {
  auto ret = city_and_state.substr(0, city_and_state.rfind(','));
  trim(ret);
  return ret;
}

static inline std::string ExtractState(const std::string &city_and_state) {
  auto ret = city_and_state.substr(1 + city_and_state.rfind(','));
  trim(ret);
  return ret;
}

static inline std::string FormatTime(int64_t time) {
  return fmt::format("{:02}:{:02}", time / 100, time % 100);
}

#endif  // HYPERSPECIAL_FLIGHTS_UTIL_H
