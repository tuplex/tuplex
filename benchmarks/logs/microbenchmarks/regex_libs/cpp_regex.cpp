#define PCRE2_CODE_UNIT_WIDTH 8

#include <chrono>
#include <fstream>
#include <vector>

#include <cstdio>
#include <cstring>

#include <pcre2.h>

const char* str_pattern = "^(\\S+) \\S+ \\S+ \\[[\\w:/]+\\s[+\\-]\\d{4}\\] \"\\S+ \\S+\\s*\\S*\\s*\" \\d{3} \\S+";
const int num_fields = 1;

int main(int argc, char **argv)
{
  // basic verify arguments
  if(argc != 2) {
    printf("Usage: ./tester <inputfile>\n");
    return 1;
  }

  // open and read input into memory
  std::vector<std::string> input;
  std::ifstream ifs(argv[1]);
  for(std::string line; std::getline(ifs, line);) {
    input.push_back(line);
  }

  // compile regex
  int errornumber;
  PCRE2_SIZE erroroffset;
  pcre2_code *re = pcre2_compile(
      (PCRE2_SPTR)str_pattern,
      PCRE2_ZERO_TERMINATED,
      0,
      &errornumber,
      &erroroffset,
      NULL);
  pcre2_jit_compile(re, PCRE2_JIT_COMPLETE);

  if(re == nullptr) {
    PCRE2_UCHAR buffer[256];
    pcre2_get_error_message(errornumber, buffer, sizeof(buffer));
    printf("PCRE2 compilation failed at offset %d: %s\n", (int)erroroffset,
      buffer);
    exit(1);
  }


  // allocate output space
  int *res = new int[2*input.size()];
  int num_failed = 0;
  int num_success = 0;

  // loop over input and run match
  auto start = std::chrono::high_resolution_clock::now();
  for(const auto& log: input) {
    // create block to hold match TODO: try with this moved in
    pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(re, NULL);

    // run match
    PCRE2_SPTR subject = (PCRE2_SPTR)log.c_str();
    PCRE2_SIZE subject_length = (PCRE2_SIZE)log.length();
    int rc = pcre2_jit_match(
        re,
        subject,
        subject_length,
        0,
        0,
        match_data,
        NULL);

    // record output
    if(rc == num_fields+1) { // successful match
      PCRE2_SIZE *ovector = pcre2_get_ovector_pointer(match_data);
      res[2*num_success] = ovector[2];
      res[2*num_success + 1] = ovector[3] - ovector[2];
      num_success++;
    } else {
      num_failed++;
    }
    pcre2_match_data_free(match_data);   /* Release memory used for the match */
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop-start);

  pcre2_code_free(re);

  printf("{\"executionTime\": %f, \"num_success\":%d, \"num_failed\":%d}\n", duration.count()/((double)1000000), num_success, num_failed);
}
