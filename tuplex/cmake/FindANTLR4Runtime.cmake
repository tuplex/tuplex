# FindANTLR4Runtime
# (c) L.Spiegelberg
# finds runtime, e.g. installed via brew install antlr4-cpp-runtime

# only under linux?
#find_package(PkgConfig)
#pkg_check_modules(PC_ANTLR4Runtime QUIET ANTLR4Runtime)

set (CMAKE_CXX_STANDARD 14)

# find include (is e.g. in /usr/local/include/antlr4-runtime/antlr4-runtime.h
find_path(ANTLR4Runtime_INCLUDE_DIR NAMES "antlr4-runtime.h" PATH_SUFFIXES "antlr4-runtime")

# find lib
find_library(ANTLR4Runtime_LIB antlr4-runtime)

set(ANTLR4Runtime_VERSION ${PC_ANTLR4Runtime_VERSION})


mark_as_advanced(ANTLR4Runtime_FOUND ANTLR4Runtime_INCLUDE_DIR ANTLR4Runtime_LIB ANTLR4Runtime_VERSION)

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