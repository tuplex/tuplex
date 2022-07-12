#define PCRE2_CODE_UNIT_WIDTH 8

#include <chrono>
#include <fstream>
#include <vector>

#include <cstdio>
#include <cstring>

#include <pcre2.h>
#include "pcre-8.44/pcrecpp.h"
#include <re2/re2.h>

const char* str_pattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+)\\s*(\\S*)\\s*\" (\\d{3}) (\\S+)";
const int num_fields = 9;

std::vector<std::string> input;
std::vector<std::vector<int>> output;
int num_failed;
int num_success;

#define OVECCOUNT 9

std::chrono::microseconds run_pcre2(bool jit) {
  // variables
  pcre2_code *re;
  PCRE2_SPTR pattern;     /* PCRE2_SPTR is a pointer to unsigned code units of */
  PCRE2_SPTR subject;     /* the appropriate width (in this case, 8 bits). */

  int errornumber;
  int rc;

  PCRE2_SIZE erroroffset;
  PCRE2_SIZE *ovector;
  PCRE2_SIZE subject_length;

  pcre2_match_data *match_data;

  // set up query
  pattern = (PCRE2_SPTR)str_pattern;

  // compile query
  re = pcre2_compile(
      pattern,
      PCRE2_ZERO_TERMINATED,
      0,
      &errornumber,
      &erroroffset,
      NULL);
  if(jit) {
    pcre2_jit_compile(re, PCRE2_JIT_COMPLETE);
  }


  if(re == nullptr) {
    PCRE2_UCHAR buffer[256];
    pcre2_get_error_message(errornumber, buffer, sizeof(buffer));
    printf("PCRE2 compilation failed at offset %d: %s\n", (int)erroroffset,
      buffer);
    exit(1);
  }


  // create block to hold match
  match_data = pcre2_match_data_create_from_pattern(re, NULL);

  auto start = std::chrono::high_resolution_clock::now();
  for(const auto& log: input) {
    subject = (PCRE2_SPTR)log.c_str();
    subject_length = (PCRE2_SIZE)strlen((char*)subject);
    // just-in-time?
    if(jit) {
      rc = pcre2_jit_match(
          re,
          subject,
          subject_length,
          0,
          0,
          match_data,
          NULL);
    } else {
      rc = pcre2_match(
          re,                   /* the compiled pattern */
          subject,              /* the subject string */
          subject_length,       /* the length of the subject */
          0,                    /* start at offset 0 in the subject */
          0,                    /* default options */
          match_data,           /* block for storing the result */
          NULL);                /* use default match context */
    }
    if(rc == num_fields+1) { // successful match
      ovector = pcre2_get_ovector_pointer(match_data);
      for(int i=1; i<rc; i++) {
        // TODO: think about /g flag, etc
        PCRE2_SPTR substring_start = subject + ovector[2*i];
        PCRE2_SIZE substring_length = ovector[2*i+1] - ovector[2*i];
				//printf("%2d: %.*s\n", i, (int)substring_length, (char *)substring_start);
      }
      num_success++;
    } else {
      //printf("FAIL: %d\n", rc);
      num_failed++;
    }
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop-start);

  pcre2_code_free(re);
  pcre2_match_data_free(match_data);   /* Release memory used for the match */
  return duration;
}

std::chrono::microseconds run_re2() {
  re2::RE2 pattern(str_pattern);
  int n = pattern.NumberOfCapturingGroups();

  // set up storage for results
  re2::RE2::Arg* args = new re2::RE2::Arg[n];
  re2::RE2::Arg** arg_ptrs = new re2::RE2::Arg*[n];
  re2::StringPiece* res = new re2::StringPiece[n];
  for(int i=0; i<n; i++) { args[i] = &res[i]; arg_ptrs[i] = &args[i]; }

  auto start = std::chrono::high_resolution_clock::now();
  for(const auto& log: input) {
    if(RE2::FullMatchN(re2::StringPiece(log), pattern, arg_ptrs, n)) {
      for(int i=0; i<n; i++) {
        // TODO: think about /g flag, etc
        const char* substring_start = res[i].begin();
        size_t substring_length = res[i].size();
      }
      num_success++;
    } else {
      num_failed++;
    }
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop-start);

  delete[] args;
  delete[] arg_ptrs;
  delete[] res;
  return duration;
}

int main(int argc, char **argv)
{
  int num_types = 3;
  const char* types[] = {"pcre2", "pcre2jit", "re2"};

  int type = 3;
  if(argc != 3) {
    printf("Usage: ./tester <type> <inputfile>\n");
    return 1;
  }

  // get run type
  for(int i=0; i<num_types; i++) {
    if(strcmp(argv[1], types[i]) == 0) {
      type = i;
      break;
    }
  }

  if(type == 3) {
    printf("Invalid type. Options:\n");
    for(int i=0; i<num_types; i++) {
      printf(" - %s\n", types[i]);
    }
    return 1;
  }

  // read in file
  std::ifstream ifs(argv[2]);
  for(std::string line; std::getline(ifs, line);) {
    input.push_back(line);
  }

  // run the test
  std::chrono::microseconds run_time;
  if(type == 0) {
    run_time = run_pcre2(false);
  } else if (type == 1) {
    run_time = run_pcre2(true);
  } else {
    run_time = run_re2();
  }

  printf("%s: %ldus\n", types[type], run_time.count());
  printf("(num_success, num_failed) = (%d, %d)\n", num_success, num_failed);
}
