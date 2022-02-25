# Flights experimental hyperspecialization benchmark/experiment

cmake_minimum_required(VERSION 3.15)
project(hyperflights)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)

# taken from https://github.com/tunabrain/tungsten/blob/master/cmake/OptimizeForArchitecture.cmake
# to automatically optimize architecture flags
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

# AVX does not do much benefit at the moment, but breaks compilation on some platforms.
# Disabled for now until AVX becomes important enough to reconsider.
set(EMBREE_MAX_ISA "SSE4.2" CACHE STRING "Selects highest ISA to support.")
set(USE_AVX FALSE CACHE BOOL "Use AVX.")
set(USE_AVX2 FALSE CACHE BOOL "Use AVX2.")

include(OptimizeForArchitecture)
OptimizeForArchitecture()
if (USE_AVX)
    message(STATUS "Compiling with AVX support")
    set(__AVX__ 1)
elseif (USE_SSE4_2)
    message(STATUS "Compiling with SSE4.2 support")
endif()

add_compile_options(-msse4.2)

# g++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o basic basic.cpp -ldl
add_executable(runner src/runner.cc)

add_library(process_row_orig SHARED src/process_row/process_row_orig.cc)
set_target_properties(process_row_orig PROPERTIES SUFFIX ".so")
set_target_properties(process_row_orig PROPERTIES PREFIX "")

# aggregate weather query
add_library(agg_weather_general SHARED src/agg_query/agg_general.cc)
set_target_properties(agg_weather_general PROPERTIES SUFFIX ".so")
set_target_properties(agg_weather_general PROPERTIES PREFIX "")

add_library(agg_weather_specialized SHARED src/agg_query/agg_specialized.cc)
set_target_properties(agg_weather_specialized PROPERTIES SUFFIX ".so")
set_target_properties(agg_weather_specialized PROPERTIES PREFIX "")

# aggregate weather query / STL map version
add_library(agg_weather_general_stl SHARED src/agg_query_stlmap/agg_general.cc)
set_target_properties(agg_weather_general_stl PROPERTIES SUFFIX ".so")
set_target_properties(agg_weather_general_stl PROPERTIES PREFIX "")

add_library(agg_weather_specialized_stl SHARED src/agg_query_stlmap/agg_specialized.cc)
set_target_properties(agg_weather_specialized_stl PROPERTIES SUFFIX ".so")
set_target_properties(agg_weather_specialized_stl PROPERTIES PREFIX "")


# aggregate weather query specialized for April / STL map version
add_library(agg_weather_general_month SHARED src/agg_query_month/agg_general.cc)
set_target_properties(agg_weather_general_month PROPERTIES SUFFIX ".so")
set_target_properties(agg_weather_general_month PROPERTIES PREFIX "")

add_library(agg_weather_specialized_month SHARED src/agg_query_month/agg_specialized.cc)
set_target_properties(agg_weather_specialized_month PROPERTIES SUFFIX ".so")
set_target_properties(agg_weather_specialized_month PROPERTIES PREFIX "")

#target_link_libraries(runner_orig ${CMAKE_DL_LIBS}) # -ldl on linux
#target_compile_definitions(runner_orig PUBLIC PR_VERSION=0)
#
#add_executable(runner_constant src/runner.cc)
#target_link_libraries(runner_constant ${CMAKE_DL_LIBS}) # -ldl on linux
#target_compile_definitions(runner_constant PUBLIC PR_VERSION=1)
#
#add_executable(runner_narrow src/runner.cc)
#target_link_libraries(runner_narrow ${CMAKE_DL_LIBS}) # -ldl on linux
#target_compile_definitions(runner_narrow PUBLIC PR_VERSION=2)
#
#
## create shared libraries for the different modes
## g++ -shared -fPIC -O3 -o process_row_orig.so process_row_orig.cpp
## g++ -shared -fPIC -O3 -o process_row_constant.so process_row_constant.cpp
## g++ -shared -fPIC -O3 -o process_row_narrow.so process_row_narrow.cpp
#add_library(process_row_orig SHARED src/process_row/process_row_orig.cc)
#set_target_properties(process_row_orig PROPERTIES SUFFIX ".so")
#set_target_properties(process_row_orig PROPERTIES PREFIX "")
#add_library(process_row_constant SHARED src/process_row/process_row_constant.cc)
#set_target_properties(process_row_constant PROPERTIES SUFFIX ".so")
#set_target_properties(process_row_constant PROPERTIES PREFIX "")
#add_library(process_row_narrow SHARED src/process_row/process_row_narrow.cc)
#set_target_properties(process_row_narrow PROPERTIES SUFFIX ".so")
#set_target_properties(process_row_narrow PROPERTIES PREFIX "")
