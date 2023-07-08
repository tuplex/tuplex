# FindANTLR4Runtime
# (c) L.Spiegelberg
# finds runtime, e.g. installed via brew install antlr4-cpp-runtime

# for try_run need 3.25+
cmake_minimum_required(VERSION 3.25 FATAL_ERROR)

# only under linux?
#find_package(PkgConfig)
#pkg_check_modules(PC_ANTLR4Runtime QUIET ANTLR4Runtime)

# set (CMAKE_CXX_STANDARD 17)

# find include (is e.g. in /usr/local/include/antlr4-runtime/antlr4-runtime.h
find_path(ANTLR4Runtime_INCLUDE_DIR NAMES "antlr4-runtime.h" PATH_SUFFIXES "antlr4-runtime")

# find lib
find_library(ANTLR4Runtime_LIB antlr4-runtime)

set(ANTLR4Runtime_VERSION ${PC_ANTLR4Runtime_VERSION})

# version empty? read from header file
if(NOT ANTLR4Runtime_VERSION MATCHES [0-9]+.[0-9]+.[0-9]+)
    set(ANTLR4Runtime_VERSION_FILE "${ANTLR4Runtime_INCLUDE_DIR}/Version.h")

    # this file exists only for Antlr4.11+, for older antlr versions, use runtime metadata
    # check therefore first whether Version.h file exists
    if(EXISTS ${ANTLR4Runtime_VERSION_FILE})
        file(READ ${ANTLR4Runtime_VERSION_FILE} FILE_CONTENTS)
        string(REGEX MATCH "VERSION_MAJOR ([0-9]*)" _ ${FILE_CONTENTS})
        set(ver_major ${CMAKE_MATCH_1})
        string(REGEX MATCH "VERSION_MINOR ([0-9]*)" _ ${FILE_CONTENTS})
        set(ver_minor ${CMAKE_MATCH_1})
        string(REGEX MATCH "VERSION_PATCH ([0-9]*)" _ ${FILE_CONTENTS})
        set(ver_patch ${CMAKE_MATCH_1})
        set(ANTLR4Runtime_VERSION "${ver_major}.${ver_minor}.${ver_patch}")
    else()
        # determine using runtime metadata
        # c++ detect.cc -o detect -I/usr/local/include/antlr4-runtime -L/usr/local/lib/ -lantlr4-runtime -std=c++17 2>/dev/null && ./detect
        # with detect.cc
        # #include <antlr4-runtime/antlr4-runtime.h>
        # #include <iostream>
        #
        # int main() {
        #   using namespace std;
        #   cout<<antlr4::RuntimeMetaData::getRuntimeVersion()<<endl;
        #   return 0;
        # }

        try_run(ANTLR4Runtime_RUN_RES ANTLR4Runtime_COMPILE_RES SOURCE_FROM_CONTENT detect.cc "
#include <antlr4-runtime.h>
#include <iostream>

int main() {
  using namespace std;
  cout<<antlr4::RuntimeMetaData::getRuntimeVersion()<<endl;
  return 0;
}
    " LINK_LIBRARIES ${ANTLR4Runtime_LIB}
                CMAKE_FLAGS
                "-DINCLUDE_DIRECTORIES=${ANTLR4Runtime_INCLUDE_DIR}"
                COMPILE_OUTPUT_VARIABLE ANTLR4Runtime_compile_log
                RUN_OUTPUT_VARIABLE ANTLR4Runtime_run_log)
        if(ANTLR4Runtime_RUN_RES STREQUAL "0")
            set(ANTLR4Runtime_VERSION "${ANTLR4Runtime_run_log}")
            string(STRIP ${ANTLR4Runtime_VERSION} ANTLR4Runtime_VERSION)
        else()
            set(ANTLR4Runtime_VERSION "unknown")
        endif()
    endif()
endif()

mark_as_advanced(ANTLR4Runtime_FOUND ANTLR4Runtime_INCLUDE_DIR ANTLR4Runtime_LIB ANTLR4Runtime_VERSION ANTLR4Runtime_VERSION)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ANTLR4Runtime
        REQUIRED_VARS ANTLR4Runtime_INCLUDE_DIR ANTLR4Runtime_LIB
        VERSION_VAR ANTLR4Runtime_VERSION
)

if(ANTLR4Runtime_FOUND)
    get_filename_component(ANTLR4Runtime_INCLUDE_DIRS ${ANTLR4Runtime_INCLUDE_DIR} DIRECTORY)

    # fix include /usr/local/include
    # add parent to include (to allow for antlr4-runtime/antlr4-runtime
    include_directories(include ${ANTLR4Runtime_INCLUDE_DIRS})
endif()