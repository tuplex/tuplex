# (c) 2017 Leonhard Spiegelberg
cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
message(STATUS "Using language versions C++${CMAKE_CXX_STANDARD}")

option(BUILD_WITH_AWS "Build Tuplex with AWS support. This will include access to S3 and the AWS Lambda execution engine backend. Requires AWS C++ SDK" ON)

# build with AWS support
if(BUILD_WITH_AWS)
    # requires at least High Sierra (10.13)
    if(APPLE)

        # mac os version detection here
        execute_process(COMMAND bash -c "sw_vers | grep -Eo '([0-9]{1,}\\.)+[0-9]{1,}' | head -1" OUTPUT_VARIABLE MACOSX_VERSION_STRING OUTPUT_STRIP_TRAILING_WHITESPACE)
        if(NOT CMAKE_OSX_DEPLOYMENT_TARGET OR "${CMAKE_OSX_DEPLOYMENT_TARGET}" STREQUAL "")

            # check what the major OS X version is, if 10 -> build for 10.13 (lowest supported)
            string(REPLACE "." ";" VERSION_LIST ${MACOSX_VERSION_STRING})
            list(GET VERSION_LIST 0 MACOSX_VERSION_MAJOR)
            if(MACOSX_VERSION_MAJOR LESS_EQUAL 10)
                # use high sierra target per default
                set(CMAKE_OSX_DEPLOYMENT_TARGET 10.13)
            else()
                # use maj.0 as default
                set(CMAKE_OSX_DEPLOYMENT_TARGET ${MACOSX_VERSION_MAJOR}.0)
            endif()
        endif()

        message(STATUS "Using macOS target ${CMAKE_OSX_DEPLOYMENT_TARGET} to build with AWS SDK component")
        if("${CMAKE_OSX_DEPLOYMENT_TARGET}" VERSION_LESS "10.13")
            message(FATAL_ERROR "Building Tuplex with AWS SDK support on Darwin requires at least macOS 10.13 (High Sierra)")
        endif()
    endif()

    # special case: if using mac os and a brew installed aws-sdk-cpp, can't use static libs => need to force to shared_libs
    if(APPLE AND BREW_FOUND)
        # check if brewed aws-sdk-cpp -> force shared libs.
        # i.e. check brew list | grep aws-sdk-cpp
        execute_process(COMMAND bash "-c" "brew list | grep aws-sdk-cpp" OUTPUT_VARIABLE BREWED_AWSSDK RESULT_VARIABLE BREW_RET OUTPUT_STRIP_TRAILING_WHITESPACE)
        if(NOT BREWED_AWSSDK STREQUAL "")
            message(STATUS "Found brewed AWS SDK C++ installed, forcing build to use shared libs.")
            SET(BUILD_SHARED_LIBS ON FORCE)
        else()
            message(STATUS "Found custom installed AWS SDK C++ installed, if cmake fails with AWS SDK files not found consider setting BUILD_SHARED_LIBS=ON/OFF depending on your AWS SDK C++ installation")
        endif()
    endif()
    find_package(AWSSDK REQUIRED COMPONENTS s3 core)
    message(STATUS "AWS libs: ${AWSSDK_LINK_LIBRARIES}")
    message(STATUS "AWS include dirs: ${AWSSDK_INCLUDE_DIR}")
    if(AWSSDK_FOUND)
        add_definitions(-DBUILD_WITH_AWS)
    else()
        message(FATAL_ERROR "option build with AWSSDK specified, but AWS SDK was not found.")
    endif ()
endif()

# include simdjson, tutorial here https://github.com/simdjson/cmake_demo_single_file/blob/master/CMakeLists.txt
include(FetchContent)
FetchContent_Declare(
        simdjson
        GIT_REPOSITORY https://github.com/simdjson/simdjson.git
        GIT_SHALLOW TRUE
        GIT_TAG tags/v3.8.0
)

# FetchContent_MakeAvailable(simdjson)

if(NOT simdjson_POPULATED)
    FetchContent_Populate(simdjson)
    # Disable threads for simdjson
    set(SIMDJSON_ENABLE_THREADS OFF CACHE INTERNAL "Disable dimsjon threads")
    add_subdirectory(${simdjson_SOURCE_DIR} ${simdjson_BINARY_DIR})
endif()


FetchContent_Declare(lyra
GIT_REPOSITORY https://github.com/bfgroup/Lyra
GIT_TAG 1.6.1
)
FetchContent_MakeAvailable(lyra)


FetchContent_Declare(yyjson
GIT_REPOSITORY https://github.com/ibireme/yyjson
GIT_TAG 0.8.0)
FetchContent_MakeAvailable(yyjson)


set(SRCS main.cc)

# linking
add_executable(cc_github ${SRCS})
target_link_libraries(cc_github simdjson lyra yyjson ${AWSSDK_LINK_LIBRARIES})