# (c) 2017-2023 Leonhard Spiegelberg
cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

# top-level language specification
# enable c++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
message(STATUS "Using language version: C++${CMAKE_CXX_STANDARD}")

# add cmake modules from cmake folder
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")

# Tuplex build options:
# =====================

# Option on whether to use shared libraries or perform a static link.
# Must be identical to how AWS SDK was installed. E.g., when installing brew aws-sdk-cpp the default is
# shared for the AWS C++ SDK build.
# --> We use static build per default.
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(SKIP_AWS_TESTS "Skip AWS tests" ON)
option(BUILD_WITH_ORC "Build with Orc file support" OFF)
option(BUILD_NATIVE "Build with -march=native" OFF)
option(BUILD_FOR_LAMBDA "use to specify whether this is a Lambda build" OFF)
option(BUILD_FOR_CI "Build for the CI - disable tests that require AWS credentials." OFF)
option(BUILD_WITH_AWS "Build Tuplex with AWS support. This will include access to S3 and the AWS Lambda execution engine backend. Requires AWS C++ SDK" ON)
option(GENERATE_PDFS "whether to generate PDFs in Debug mode or not. Disable for faster testing speeds." OFF)
option(SHOW_EXPLICIT_WARNINGS "Show the output of #warning directives in the code (lots of output)" OFF)
option(USE_LD_GOLD "Use GNU gold linker" ON)

# helper to check whether var exists and is valid
function(ASSERT_VAR VARNAME)
    if(DEFINED ${VARNAME})
        string(COMPARE EQUAL "${${VARNAME}}" "" str_result)
        if("${str_result}")
            message(FATAL_ERROR "variable ${VARNAME} is empty string")
        endif()
    else()
        message(FATAL_ERROR "expected variable ${VARNAME} to exist.")
    endif()
endfunction()

# ninja fixes for multiple zstd generators
if(CMAKE_GENERATOR STREQUAL "Ninja")
    message(STATUS "Using ninja generator, if fails use -w dupbuild=err")
endif()

# The -fvisibility=hidden option only works for static builds.
if (NOT BUILD_SHARED_LIBS)
    set(CMAKE_CXX_VISIBILITY_PRESET hidden)
else()
    if (CMAKE_CXX_VISIBILITY_PRESET STREQUAL "hidden")
        message(FATAL_ERROR "CMAKE_CXX_VISIBILITY_PRESET=hidden is incompatible \
                           with BUILD_SHARED_LIBS.")
    endif()
endif()

# detect MacOS Version because at least 10.13 is required when building with AWS SDK
if(APPLE)
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

    message(STATUS "Detected macOS ${MACOSX_VERSION_STRING} host platform, building for deployment target ${CMAKE_OSX_DEPLOYMENT_TARGET}. To use a different deployment target either set CMAKE_OSX_DEPLOYMENT_TARGET or the environment variable MACOSX_DEPLOYMENT_TARGET to the desired version.")
endif()

# via -DVERSION_INFO a string may be passed down. --> include it somehow
if(VERSION_INFO)
    message(STATUS "Version info supplied to cmake: ${VERSION_INFO}")
    project(Tuplex VERSION "${VERSION_INFO}" DESCRIPTION "Tuplex processing framework")
else()
    message(STATUS "Building dev version")
    project(Tuplex DESCRIPTION "Tuplex processing framework")
endif()


# before writing additional cmake modules to put in cmake/, check the list of supported cmake standard modules
# available here: https://cmake.org/cmake/help/latest/manual/cmake-modules.7.html#find-modules

# uncomment to get verbose cmake output
# set(CMAKE_VERBOSE_MAKEFILE ON)

message(STATUS "additional cmake module path is ${CMAKE_MODULE_PATH}")
include("${CMAKE_SOURCE_DIR}/cmake/ucm.cmake") #handy package to manipulate compiler flags
include("${CMAKE_SOURCE_DIR}/cmake/CPM.cmake") # package manager from https://github.com/cpm-cmake/CPM.cmake
# for debug mode export all symbols
set(CMAKE_ENABLE_EXPORTS true)
include(targetLinkLibrariesWithDynamicLookup)

# this here will enable LTO (link-time optimization) globally
include(CheckIPOSupported)
check_ipo_supported(RESULT IPO_SUPPORTED)
if(IPO_SUPPORTED)
    # on Ubuntu gcc is buggy, so deactivate LTO for Linux...
    if(NOT CMAKE_SYSTEM_NAME MATCHES "Linux" AND "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
        message(STATUS "Enabling interprocedural optimization for built")
    endif()
else()
    message(WARNING "target does not support interprocedural optimization/link time optimization.")
endif()

# Check if ccache exists to speed up compilation when switching branches
# taken from https://invent.kde.org/utilities/konsole/-/merge_requests/26?tab=diffs
find_program(CCACHE_FOUND "ccache")
set(CCACHE_SUPPORT ON CACHE BOOL "Enable ccache support")
if (CCACHE_FOUND AND CCACHE_SUPPORT)
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" # GNU is GNU GCC
            OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        # without this compiler messages in `make` backend would be uncolored
        ucm_add_flags("-fdiagnostics-color=auto")
    endif()
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "ccache")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK "ccache")
endif()

set(CMAKE_MACOSX_RPATH 1) # fix for gtest warning

# translate to C++ flags
if(SKIP_AWS_TESTS)
    add_definitions(-DSKIP_AWS_TESTS)
endif()

if(BUILD_WITH_ORC)
    add_definitions(-DBUILD_WITH_ORC)
endif()

# add -Werror=return-type  to turn missing returns into errors!!!
macro(append_if list condition var)
    if (${condition})
        list(APPEND ${list} ${var})
    endif()
endmacro()

# manipulate warnings
if (NOT MSVC)
    # supress "#warning" if desired, else enable all
    if(SHOW_EXPLICIT_WARNINGS)
        ucm_add_flags("-Wall")
    else()
        ucm_add_flags("-Wno-cpp")
    endif()

    # make no return warning into an error because it easily leads to bugs
    # https://foonathan.net/2018/10/cmake-warnings/
    ucm_add_flags("-Werror=return-type")
else()
    message(FATAL_ERROR "msvc not supported yet")
endif()

###########################################################################
# (0) add additional cmake modules
###########################################################################

# global config, brew there?
FIND_PROGRAM(BREW_FOUND "brew")
if(BREW_FOUND)
    if(APPLE)
        message(STATUS "Found brew on MacOS")
    elseif(UNIX)
        message(STATUS "Found brew on Unix")
    endif()
endif()
enable_testing()

# mainly from https://github.com/AdaCore/z3/blob/master/CMakeLists.txt
message(STATUS "CMake generator: ${CMAKE_GENERATOR}")
set(available_build_types Debug Release RelWithDebInfo MinSizeRel tsan asan)
if(DEFINED CMAKE_CONFIGURATION_TYPES)
    # multi-configuration build, i.e. MSVC or Xcode
    message(STATUS "Available configurations: ${CMAKE_CONFIGURATION_TYPES}")
else()
    # single-configuration build, i.e. Unix Makefiles, Ninja...
    if(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE Debug) # use Debug per default?
        message(STATUS "CMAKE_BUILD_TYPE is not set. Using ${CMAKE_BUILD_TYPE}")
        message(STATUS "Available build types are: ${available_build_types}")

        # Provide drop down menu options in cmake-gui
        set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS ${available_build_types})
    endif()
    message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
endif()

# add to debug/gcc stack protector
# -fstack-protector-strong

# Options:
# build for CI (disable some tests)
if(BUILD_FOR_CI)
    add_definitions(-DBUILD_FOR_CI)
endif()

## Protobuf
if(BUILD_WITH_AWS)
    # building with AWS backend support?
    # communication with AWS Lambda happens via protobuf, i.e. make sure protobuf compiler
    # is installed
    set(PROTOBUF_REQUIRED True)
endif()

if(BUILD_WITH_ORC)
    # ORC requires protobuf for schema
    set(PROTOBUF_REQUIRED True)
endif()

# if protobuf is required, add as lib here.
if(PROTOBUF_REQUIRED)
    message(STATUS "Build requires Protobuf")
    set(Protobuf_USE_STATIC_LIBS ON)
    # https://github.com/protocolbuffers/protobuf/issues/12637
    find_package(Protobuf CONFIG)
    if(NOT Protobuf_FOUND)
        find_package(Protobuf REQUIRED)
    endif()

    if(Protobuf_LIBRARY)
    else()
        get_target_property(Protobuf_LIBRARY protobuf::libprotobuf LOCATION)
    endif()
    cmake_path(GET Protobuf_LIBRARY PARENT_PATH Protobuf_LIBRARY_DIR)
    cmake_path(GET Protobuf_LIBRARY_DIR PARENT_PATH Protobuf_HOME)

    message(STATUS "Protobuf home is ${Protobuf_HOME}")
    assert_var(Protobuf_HOME)

    # newer protobuf has abseil dependency, amend protobuf libs accordingly because protobuf is shipped in
    # a non-fixed state (see https://github.com/protocolbuffers/protobuf/issues/12637)
    # there's a bug in cmake for cmake < 3.27 where version is detected wrongly as 4.x -> fix
    if((Protobuf_VERSION VERSION_GREATER_EQUAL "3.22" AND Protobuf_VERSION VERSION_LESS "4.0") OR (Protobuf_VERSION VERSION_GREATER_EQUAL "4.3.22" AND Protobuf_VERSION VERSION_LESS "5.0.0") OR (Protobuf_VERSION VERSION_GREATER_EQUAL "22.0"))
        find_package(absl REQUIRED)
        find_package(utf8_range REQUIRED)
        set(protobuf_ABSL_USED_TARGETS
                absl::absl_check
                absl::absl_log
                absl::algorithm
                absl::base
                absl::bind_front
                absl::bits
                absl::btree
                absl::cleanup
                absl::cord
                absl::core_headers
                absl::debugging
                absl::die_if_null
                absl::dynamic_annotations
                absl::flags
                absl::flat_hash_map
                absl::flat_hash_set
                absl::function_ref
                absl::hash
                absl::layout
                absl::log_initialize
                absl::log_severity
                absl::memory
                absl::node_hash_map
                absl::node_hash_set
                absl::optional
                absl::span
                absl::status
                absl::statusor
                absl::strings
                absl::synchronization
                absl::time
                absl::type_traits
                absl::utility
                absl::variant
                utf8_range::utf8_validity
        )
        list(APPEND Protobuf_LIBRARIES ${protobuf_ABSL_USED_TARGETS})
    endif()
endif()



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
    find_package(AWSSDK REQUIRED COMPONENTS s3 core lambda transfer)
    message(STATUS "AWS libs: ${AWSSDK_LINK_LIBRARIES}")
    message(STATUS "AWS include dirs: ${AWSSDK_INCLUDE_DIR}")
    if(AWSSDK_FOUND)
        add_definitions(-DBUILD_WITH_AWS)
    else()
        message(FATAL_ERROR "option build with AWSSDK specified, but AWS SDK was not found.")
    endif ()
endif()

if(GENERATE_PDFS)
    message(STATUS "Tuplex configured to emit PDF files for various AST stages")
    add_definitions(-DGENERATE_PDFS)
else()
    message(STATUS "PDF generation for ASTs and logical plans disabled. Enable by setting -DGENERATE_PDFS=ON.")
endif()

###########################################################################
# (1) supported compilers and fixes
###########################################################################

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # require at least gcc 6
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 6.0)
        message(FATAL_ERROR "GCC version must be at least 6.0!")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    # require at least clang 5
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.0)
        message(FATAL_ERROR "Clang version must be at least 5.0!")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! ${CMAKE_CXX_COMPILER_ID} Compilation has only been tested with Clang and GCC.")
endif()

#GCC fixes (why does this compiler suck so much???)
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # set AR and RANLIB to gcc-ar and gcc-ranglib if found
    find_program(GCC_AR "gcc-ar")
    find_program(GCC_RANLIB "gcc-ranlib")
    if(GCC_AR AND GCC_RANLIB)
        # important to set like this
        # i.e. this is required on Linux to enable link-time-optimization
        SET(CMAKE_AR "gcc-ar")
        SET(CMAKE_RANLIB "gcc-ranlib")
    else()
        message(FATAL_ERROR "could not find gcc-ar or gcc-ranlib. Make sure they are installed and symlinked. Leaving them at their defaults (ar: ${CMAKE_AR}, ranlib: ${CMAKE_RANLIB}) will produce lto errors in Release build.")
    endif ()
endif()


###########################################################################
# (2) global flags
###########################################################################
# i.e., manipulate here using ucm

# set here initial compiler flags
# DO NOT USE MARCH=NATIVE for wheels... => option should be disabled per default
# use march=native if available and NOT building wheels

# specific Release flags, i.e. targeting loop unrolling.
ucm_add_flags(-m64 -O3 -funroll-loops CONFIG Release)

# for GCC add stack protector
# -fstack-protector-strong
if("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
    ucm_add_flags(-fstack-protector-strong)
endif()

include(CheckCXXCompilerFlag)
if(BUILD_NATIVE AND NOT BUILD_FOR_LAMBDA) # for lambda build, specific architecture has been targeted.
    CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
    if(COMPILER_SUPPORTS_MARCH_NATIVE)
        ucm_add_flags(-march=native)
    endif()
    message(STATUS "Building Tuplex with native optimizations")
endif()

find_package(Threads REQUIRED)
# enable PIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# for release mode, don't use Ofast unless you're ok to break NAN/INF compatibility.
if(BUILD_FOR_LAMBDA)
    ucm_add_flags(-march=haswell)
endif()

# add compiler flags for specific SIMD instructions
include(FindSSE)
include(FindSIMD)
CHECK_FOR_SSE42()
CHECK_FOR_AVX()
# add compiler flags
if(HAVE_SSE42)
    message(STATUS "Found SSE4.2 support, adding -msse4.2")
    # no msvc support, so use clang/gcc flag approach!
    add_compile_options("-msse4.2")
endif()
if(HAVE_AVX)
    # only add avx (because not any architecture supports it) if native build is enabled or build for lambda
    # because Haswell supports both AVX and AVX2!
    if(BUILD_NATIVE OR BUILD_FOR_LAMBDA)
        message(STATUS "Found AVX support, adding -mavx")
        # no msvc support, so use clang/gcc flag approach!
        add_compile_options("-mavx")
    endif()
else()
    message(STATUS "no avx support, build with -DBUILD_NATIVE=ON to enable")
endif()

###########################################################################
# (3) enable sanitizers if necessary
###########################################################################
# enable address sanitizaton
# in debug build
# check https://github.com/google/sanitizers for info
# under MAC OS X also have export ASAN_OPTIONS=detect_leaks=1 enabled!
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -fsanitize=address")

## set ASAN_OPTIONS=halt_on_error=0
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -fsanitize=address -fPIE -g -O1")

# run also especially https://github.com/google/sanitizers/wiki/AddressSanitizerLeakSanitizer

# further https://github.com/google/sanitizers/wiki/MemorySanitizer

# clang sanitizers
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fPIE -g -O1")

# note this will slowdown execution by a lot.
# uncomment this to enable debug mode in thread sanitizer...
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -fsanitize=thread -fPIE -g -O1")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -fsanitize=thread -O1")

# from http://www.stablecoder.ca/2018/02/01/analyzer-build-types.html
# Build Types
set(CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE}
        CACHE STRING "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel tsan asan lsan msan ubsan"
        FORCE)

# ThreadSanitizer
set(CMAKE_C_FLAGS_TSAN
        "-fsanitize=thread -g -O1"
        CACHE STRING "Flags used by the C compiler during ThreadSanitizer builds."
        FORCE)
set(CMAKE_CXX_FLAGS_TSAN
        "-fsanitize=thread -g -O1"
        CACHE STRING "Flags used by the C++ compiler during ThreadSanitizer builds."
        FORCE)

# AddressSanitize
set(CMAKE_C_FLAGS_ASAN
        "-fsanitize=address -fsanitize-recover=address -fno-omit-frame-pointer -g -O1"
        CACHE STRING "Flags used by the C compiler during AddressSanitizer builds."
        FORCE)
set(CMAKE_CXX_FLAGS_ASAN
        "-fsanitize=address -fsanitize-recover=address -fno-optimize-sibling-calls -fsanitize-address-use-after-scope -fno-omit-frame-pointer -g -O1"
        CACHE STRING "Flags used by the C++ compiler during AddressSanitizer builds."
        FORCE)

ucm_set_flags(-fsanitize=address -fsanitize-recover=address -fno-optimize-sibling-calls -fsanitize-address-use-after-scope -fno-omit-frame-pointer -g -O1 CONFIG asan)

# LeakSanitizer
set(CMAKE_C_FLAGS_LSAN
        "-fsanitize=leak -fno-omit-frame-pointer -g -O1"
        CACHE STRING "Flags used by the C compiler during LeakSanitizer builds."
        FORCE)
set(CMAKE_CXX_FLAGS_LSAN
        "-fsanitize=leak -fno-omit-frame-pointer -g -O1"
        CACHE STRING "Flags used by the C++ compiler during LeakSanitizer builds."
        FORCE)

# MemorySanitizer
set(CMAKE_C_FLAGS_MSAN
        "-fsanitize=memory -fno-optimize-sibling-calls -fsanitize-memory-track-origins=2 -fno-omit-frame-pointer -g -O2"
        CACHE STRING "Flags used by the C compiler during MemorySanitizer builds."
        FORCE)
set(CMAKE_CXX_FLAGS_MSAN
        "-fsanitize=memory -fno-optimize-sibling-calls -fsanitize-memory-track-origins=2 -fno-omit-frame-pointer -g -O2"
        CACHE STRING "Flags used by the C++ compiler during MemorySanitizer builds."
        FORCE)

# UndefinedBehaviour
set(CMAKE_C_FLAGS_UBSAN
        "-fsanitize=undefined"
        CACHE STRING "Flags used by the C compiler during UndefinedBehaviourSanitizer builds."
        FORCE)
set(CMAKE_CXX_FLAGS_UBSAN
        "-fsanitize=undefined"
        CACHE STRING "Flags used by the C++ compiler during UndefinedBehaviourSanitizer builds."
        FORCE)

###########################################################################
# (4) output directories
###########################################################################
# set output paths depending on configuration type
# in single-config builds (i.e. make/ninja) set output dir to be dist
# in multi config name after config type
# the final build will be contained within folder dist/ for easier lookup
#if(MSVC OR "Xcode" MATCHES ${CMAKE_GENERATOR})
#    foreach( OUTPUTCONFIG ${CMAKE_CONFIGURATION_TYPES})
#        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/${OUTPUTCONFIG}/bin)
#        set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/${OUTPUTCONFIG}/lib)
#        set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/${OUTPUTCONFIG}/lib)
#        set(DIST_DIR_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/${OUTPUTCONFIG})
#    endforeach()
#else()
#    set(DIST_DIR ${CMAKE_BINARY_DIR}/dist)
#    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${DIST_DIR}/lib)
#    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${DIST_DIR}/lib)
#    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${DIST_DIR}/bin)
#endif()


set(DIST_DIR ${CMAKE_BINARY_DIR}/dist)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${DIST_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${DIST_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${DIST_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG ${DIST_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_DEBUG ${DIST_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${DIST_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE ${DIST_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_RELEASE ${DIST_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${DIST_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_MINSIZEREL ${DIST_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_MINSIZEREL ${DIST_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL ${DIST_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELWITHDEBINFO ${DIST_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_RELWITHDEBINFO ${DIST_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${DIST_DIR}/bin)

###########################################################################
# (5) add subprojects
###########################################################################
# add all subcomponents, so doc target can build.
add_compile_definitions(SPDLOG_FMT_EXTERNAL)
add_compile_definitions(FMT_HEADER_ONLY=1)
add_compile_definitions(PCRE2_CODE_UNIT_WIDTH=8)


# add boost & boost python ==> globally added because of its quirks
# this solution should work across versions...

# use 3.12 find_package(Python3 ...)

# because users might want to specify their python3 version, use alternative finding mechanism
set(PYTHON3_VERSION "" CACHE STRING "use to specify which version, e.g. 3.6 or python3.6")
if(DEFINED ENV{PYTHON3_VERSION})
    set(PYTHON3_VERSION "$ENV{PYTHON3_VERSION}") # can use env variable as well!
endif()


# check if a specific Python executable was set
if(PYTHON_EXECUTABLE AND NOT PYTHON3_EXECUTABLE)
    set(PYTHON3_EXECUTABLE ${PYTHON_EXECUTABLE})
endif()
if(PYTHON3_EXECUTABLE)
    set(Python3_EXECUTABLE ${Python3_EXECUTABLE})
    message(STATUS "Using specific python executable ${PYTHON3_EXECUTABLE}")
    unset(PYTHON3_VERSION)

    # get version from executable
    execute_process (COMMAND "${PYTHON3_EXECUTABLE}" -c "import sys;print('{}.{}.{}'.format(sys.version_info.major,sys.version_info.minor,sys.version_info.micro))"
            RESULT_VARIABLE _result
            OUTPUT_VARIABLE PYTHON3_VERSION
            OUTPUT_STRIP_TRAILING_WHITESPACE)
    message(STATUS "Detected version of python executable to be ${PYTHON3_VERSION}")


    # Python version to get root dir
    # python3 -c "import sys; from pathlib import Path; print(Path(sys.executable).resolve().parent.parent)"
    execute_process (COMMAND "${PYTHON3_EXECUTABLE}" -c "import sys; from pathlib import Path; print(Path(sys.executable).resolve().parent.parent)"
            RESULT_VARIABLE _result
            OUTPUT_VARIABLE Python3_ROOT_DIR
            OUTPUT_STRIP_TRAILING_WHITESPACE)
    # Pure CMAKE version:
    # get_filename_component(Python3_ROOT_DIR ${PYTHON3_EXECUTABLE}/../.. ABSOLUTE)
    message(STATUS "Detected Python3 Root dir to be: ${Python3_ROOT_DIR}")
endif()


# Computes the realtionship between two version strings.  A version
# string is a number delineated by '.'s such as 1.3.2 and 0.99.9.1.
# You can feed version strings with different number of dot versions,
# and the shorter version number will be padded with zeros: 9.2 <
# 9.2.1 will actually compare 9.2.0 < 9.2.1.
#
# Input: a_in - value, not variable
#        b_in - value, not variable
#        result_out - variable with value:
#                         -1 : a_in <  b_in
#                          0 : a_in == b_in
#                          1 : a_in >  b_in
#
# Written by James Bigler.
MACRO(COMPARE_VERSION_STRINGS a_in b_in result_out)
  # Since SEPARATE_ARGUMENTS using ' ' as the separation token,
  # replace '.' with ' ' to allow easy tokenization of the string.
  STRING(REPLACE "." " " a ${a_in})
  STRING(REPLACE "." " " b ${b_in})
  SEPARATE_ARGUMENTS(a)
  SEPARATE_ARGUMENTS(b)

  # Check the size of each list to see if they are equal.
  LIST(LENGTH a a_length)
  LIST(LENGTH b b_length)

  # Note that range needs to be one less than the length as the for
  # loop is inclusive (silly CMake).
  IF(a_length LESS b_length)
    # a is shorter
    MATH(EXPR range "${a_length} - 1")
  ELSE(a_length LESS b_length)
    # b is shorter
    MATH(EXPR range "${b_length} - 1")
  ENDIF(a_length LESS b_length)

  SET(result 0)
  FOREACH(index RANGE ${range})
    IF(result EQUAL 0)
      # Only continue to compare things as long as they are equal
      LIST(GET a ${index} a_version)
      LIST(GET b ${index} b_version)
      # LESS
      IF(a_version LESS b_version)
        SET(result -1)
      ENDIF(a_version LESS b_version)
      # GREATER
      IF(a_version GREATER b_version)
        SET(result 1)
      ENDIF(a_version GREATER b_version)
    ENDIF(result EQUAL 0)
  ENDFOREACH(index)

  # Copy out the return result
  SET(${result_out} ${result})
ENDMACRO(COMPARE_VERSION_STRINGS)



# this is a macro to find python3 depending on version etc.
function(FindPython3Exe NAMES VERSION EXECUTABLE)
	# first check if appropriate program can be found by general path search
	set(NAMES_TO_SEARCH ${NAMES})
	separate_arguments(NAMES_TO_SEARCH)
	message(STATUS "names to search to find python: ${NAMES_TO_SEARCH}")
	find_program(TEMP_EXE NAMES ${NAMES_TO_SEARCH})
	message(STATUS "exe: ${TEMP_EXE}")
	if(TEMP_EXE)
            # check version (must match VERSION)
	    execute_process(COMMAND "${TEMP_EXE}" -c "import platform;print(platform.python_version())" RESULT_VARIABLE _result OUTPUT_VARIABLE TEMP_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
            # check if version matches
        if(VERSION AND TEMP_VERSION)
            compare_version_strings(${VERSION} ${TEMP_VERSION} _result)
            if(result EQUAL 0)
                message(STATUS "Found ${TEMP_EXE} with version ${TEMP_VERSION} matching desired version ${VERSION}")
                set(${EXECUTABLE} ${TEMP_EXE} PARENT_SCOPE) # write out
            endif()
        endif()
	endif()

endfunction()

# is a python3 version set?
if(PYTHON3_VERSION STREQUAL "")

    # set hints correctly
    # first check if Python3_ROOT_DIR is empty, if so when an executable is specified use it's parent dir as hint
    if(NOT Python3_ROOT_DIR OR Python3_ROOT_DIR STREQUAL "")
        if(Python3_EXECUTABLE AND NOT Python3_EXECUTABLE STREQUAL "")
            cmake_path(GET Python3_EXECUTABLE PARENT_PATH Python3_ROOT_DIR)
        endif()
    endif()


    # try first to find full Python3, if it fails find only Interpreter & Module
    find_package(Python3 COMPONENTS Interpreter Development QUIET)
    if(Python3_FOUND)
        message(STATUS "Found full python3-dev installation")
        set(Python3_Embed_FOUND TRUE)
    else()
        find_package(Python3 COMPONENTS Interpreter QUIET)
        if(Python3_FOUND)
            # python3 -c 'import distutils.sysconfig; print(distutils.sysconfig.get_python_lib(plat_specific=False,standard_lib=True))'
            # try to get get module libs at least

            # mark embed lib as not found
            unset(Python3_Embed_FOUND)
        else()
            # use interpreter find script to detect python interpreter
            include(FindPythonInterpreter)
            find_python_interpreter(VERSION 3
                    INTERPRETER_OUT_VAR PYTHON_INTERPRETER
                    VERSION_OUT_VAR PYTHON_VERSION
                    REQUIRED
            )
            message(STATUS "Found interpreter ${PYTHON_INTERPRETER} version ${PYTHON_VERSION}")
            set(Python3_EXECUTABLE ${PYTHON_INTERPRETER})
            set(PYTHON3_VERSION ${PYTHON_VERSION})
            unset(Python3_Embed_FOUND)
            set(Python3_FOUND TRUE)

            # check if embed artifacts are there...
            set(Python3_FIND_VIRTUALENV "FIRST")
            find_package(Python3 COMPONENTS Interpreter Development QUIET)
            if(Python3_FOUND)
                message(STATUS "Found full python3-dev installation")
                set(Python3_Embed_FOUND TRUE)
            endif()
        endif()
    endif()
else()
    set(Python3_FIND_VIRTUALENV "FIRST")
    message(STATUS "checking for specific Python3 version")
    # get version via regex
    if(PYTHON3_VERSION MATCHES "^([0-9]+)\\.([0-9]+)(\\.([0-9]+))?$")
        string(REGEX MATCH "^([0-9]+)\\.([0-9]+)(\\.([0-9]+))?$" VERSION_STRING "${PYTHON3_VERSION}")
        message(STATUS "Forcing python3 version to ${VERSION_STRING}")

       # get python3 maj.min
       string(REPLACE "." ";" VERSION_LIST ${VERSION_STRING})
       list(GET VERSION_LIST 0 PYTHON3_VERSION_MAJOR)
       list(GET VERSION_LIST 1 PYTHON3_VERSION_MINOR)

       #if no executable is specified, try to search for python3 executable
       FindPython3Exe("python${PYTHON3_VERSION_MAJOR}.${PYTHON3_VERSION_MINOR} python${PYTHON3_VERSION_MAJOR}" "${PYTHON3_VERSION}" Python3_EXECUTABLE)
	    # include/lib folders might not adhere to schema, therefore if manually given give that precedence
        # try first to find full Python3, if it fails find only Interpreter & Module
        find_package(Python3 ${VERSION_STRING} EXACT COMPONENTS Interpreter Development QUIET)
        if(Python3_FOUND)
            message(STATUS "Found full python3-dev installation")
            set(Python3_Embed_FOUND TRUE)
        else()
            set(Python3_FIND_STRATEGY LOCATION)
            # use more modern approach using findpython3
            set(Python3_EXECUTABLE "${PYTHON3_EXECUTABLE}")
            message(STATUS "Using exact python executable ${Python3_EXECUTABLE}")
            find_package(Python3 ${VERSION_STRING} EXACT COMPONENTS Interpreter REQUIRED)
            # python3 -c 'import distutils.sysconfig; print(distutils.sysconfig.get_python_lib(plat_specific=False,standard_lib=True))'
            # try to get get module libs at least

            # mark embed lib as not found
            unset(Python3_Embed_FOUND)
        endif()
    else()
        message(FATAL_ERROR "Could not extract python3 version string from ${PYTHON3_VERSION}")
    endif()
endif()

# get the suffix for python extension module and store in PYTHON_MODULE_EXTENSION
execute_process (COMMAND "${Python3_EXECUTABLE}" -c "from distutils import sysconfig;print(sysconfig.get_config_var('EXT_SUFFIX'))"
        RESULT_VARIABLE _result
        OUTPUT_VARIABLE PYTHON_MODULE_EXTENSION
        ERROR_QUIET
        OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Python3 extension module suffix is: ${PYTHON_MODULE_EXTENSION}")

set(Boost_NO_WARN_NEW_VERSIONS ON) # cmake 3.20 finally has a switch to kill the boost warnings...
set(BOOST_COMPONENTS iostreams thread system filesystem)
if(Python3_FOUND)
    if(Python3_VERSION_MAJOR LESS 3 OR Python3_VERSION_MINOR LESS 5)
        message(FATAL_ERROR "Tuplex requires python3.5 at least, found incompatible python ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}")
    endif()


    if(UNIX AND NOT APPLE)
        set(Boost_USE_STATIC_LIBS ON)
    endif()
    message(STATUS "Found python${Python3_VERSION} - if you'd like to change a to different python version, use -DPython3_ROOT_DIR=<prefix> or -DPYTHON3_VERSION=<major.minor.micro> or set an environment variable PYTHON3_VERSION")

    set(Boost_NO_BOOST_CMAKE ON) # findboost from cmake is buggy and does not work, explicitly disable here
    if(APPLE AND BREW_FOUND) # i.e. boost-python via brew? --> check maybe better in the future...
        set(CMAKE_FIND_PACKAGE_PREFER_CONFIG TRUE) # gets rid off annoying boost warning.
    endif()
    find_package(Boost 1.70 COMPONENTS ${BOOST_COMPONENTS} REQUIRED)

    # check if headers/libs are set.
    # at least set headers!
    # distutils.sysconfig.get_python_inc
    if("${Python3_INCLUDE_DIRS}" STREQUAL "")
        execute_process (COMMAND "${Python3_EXECUTABLE}" -c "import sysconfig; print(sysconfig.get_path('include'))"
                RESULT_VARIABLE _result
                OUTPUT_VARIABLE Python3_INCLUDE_DIRS
                ERROR_QUIET
                OUTPUT_STRIP_TRAILING_WHITESPACE)
        message(STATUS "Detected Python3 include dir to be ${Python3_INCLUDE_DIRS}")
    endif()

    # use these switches here to specialize Boost behavior
    SET(Boost_USE_STATIC_LIBS OFF)
    SET(Boost_USE_MULTITHREADED ON)
    SET(Boost_USE_STATIC_RUNTIME OFF)
else()
    message(FATAL_ERROR "Python not found, required to compile Tuplex.")
endif()

# retrieve Root dir if empty
if(NOT "${Python3_ROOT_DIR}" OR "${Python3_ROOT_DIR}" STREQUAL "")
	# python3 -c 'import sys,pathlib; print(pathlib.Path(sys.executable).parent.parent)'
       execute_process (COMMAND "${Python3_EXECUTABLE}" -c "import sys,pathlib; print(pathlib.Path(sys.executable).parent.parent)"
                RESULT_VARIABLE _result
		OUTPUT_VARIABLE Python3_ROOT_DIR
                ERROR_QUIET
                OUTPUT_STRIP_TRAILING_WHITESPACE)	
endif()

message(STATUS "Using Python3 executable ${Python3_EXECUTABLE}")
message(STATUS "Found Python3 headers in ${Python3_INCLUDE_DIRS}")
message(STATUS "Found Python3 libs in ${Python3_LIBRARIES}")
message(STATUS "Python3 Root dir is ${Python3_ROOT_DIR}")

find_package(pcre2 REQUIRED)
if(pcre2_FOUND)
    message(STATUS "Found pcre2 libs in ${PCRE2_LIBRARIES}")
    message(STATUS "Found pcre2 headers in ${PCRE2_INCLUDE_DIRS}")
endif()


# find ZSTD / ZLIB
include(ExternalProject)
set(EXTERNAL_INSTALL_LOCATION ${CMAKE_BINARY_DIR}/third_party)

# external libs to build / download
set(ZLIB_VERSION "1.2.11") # which zlib version to use
set(ZSTD_VERSION "1.5.0") # which zstd version to use
set(BUILD_AND_DOWNLOAD_ZLIB True)
set(BUILD_AND_DOWNLOAD_ZSTD True)

# find zlib first via cmake
find_package(ZLIB 1.2.11)
if(ZLIB_FOUND)
    # nothing todo
else()
    # check if apple and brewed version is available, if not download & build
    if(APPLE AND BREW_FOUND)
        # Zlib
        EXECUTE_PROCESS(COMMAND brew list zlib OUTPUT_VARIABLE BREW_ZLIB_LIST RESULT_VARIABLE BREW_ZLIB_FOUND ERROR_VARIABLE BREW_ZLIB_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
        if(BREW_ZLIB_FOUND)
            EXECUTE_PROCESS(COMMAND brew --prefix zlib OUTPUT_VARIABLE BREW_ZLIB_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
            set(ENV{ZLIB_HOME} ${BREW_ZLIB_DIR})
            set(ZLIB_HOME ${BREW_ZLIB_DIR})
            message(STATUS "Found locally installed zlib under $ENV{ZLIB_HOME}")
            # set variables
            file (TO_CMAKE_PATH "${ZLIB_HOME}" _zlib_path)
            find_library (ZLIB_LIBRARY NAMES z HINTS
                    ${_zlib_path}
                    PATH_SUFFIXES "lib" "lib64")
            if(ZLIB_LIBRARY)
                message(STATUS "zlib lib: ${ZLIB_LIBRARY}")
            endif()
            find_library (ZLIB_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}z${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
                    ${_zlib_path}
                    PATH_SUFFIXES "lib" "lib64")
            if(ZLIB_LIBRARY)
                set(ZLIB_LIBRARIES "${ZLIB_LIBRARY}")
            elseif(ZLIB_STATIC_LIB)
                set(ZLIB_LIBRARIES "${ZLIB_STATIC_LIB}")
            endif()
            message(STATUS "Zlib libraries: ${ZLIB_LIBRARIES}")
        endif()
    endif()

    if(NOT ZLIB_LIBRARY)
        message(STATUS "Could not find locally installed zlib, building third party")
        set(ZLIB_HOME "${EXTERNAL_INSTALL_LOCATION}")
        set(ZLIB_INCLUDE_DIR "${ZLIB_HOME}/include")
        set(ZLIB_STATIC_LIB "${ZLIB_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}z${CMAKE_STATIC_LIBRARY_SUFFIX}")
        set(ZLIB_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ZLIB_HOME}
                -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_LIBDIR=lib -DZLIB_BUILD_TESTS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
        ExternalProject_Add (zlib_ep
                URL "http://zlib.net/fossils/zlib-${ZLIB_VERSION}.tar.gz"
                CMAKE_ARGS ${ZLIB_CMAKE_ARGS}
                BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}")

        set(ZLIB_LIBRARIES ${ZLIB_STATIC_LIB})

        add_library(zlib INTERFACE)
        target_link_libraries(zlib INTERFACE ${ZLIB_STATIC_LIB})
        target_include_directories(zlib SYSTEM INTERFACE ${ZLIB_INCLUDE_DIR})

        add_dependencies(zlib zlib_ep)
        install(FILES "${ZLIB_STATIC_LIB}" DESTINATION "lib")
        set(ZLIB_DEPENDS "zlib_ep")
    endif()
endif()

# zstd has no cmake standard module, so manually search for it
find_package(zstd "${ZSTD_VERSION}")
if(zstd_FOUND)
    # check if zstd is defined as target
    if(TARGET zstd::libzstd_static)
        set(ZSTD_LIBRARIES "zstd::libzstd_static") # could also be libzstd_shared
    endif()
    # if not, use variables directly
    if(ZSTD_LIBRARY)
        set(ZSTD_LIBRARIES "${ZSTD_LIBRARY}")
    elseif(ZSTD_STATIC_LIB)
        set(ZSTD_LIBRARIES "${ZSTD_STATIC_LIB}")
    endif()
else()

    # check if brewed by chance, if not fetch
    if(APPLE AND BREW_FOUND)
        set(THIRDPARTY_CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}")

        # Zstd
        EXECUTE_PROCESS(COMMAND brew list zstd OUTPUT_VARIABLE BREW_ZSTD_LIST RESULT_VARIABLE BREW_ZSTD_FOUND ERROR_VARIABLE BREW_ZSTD_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
        if(BREW_ZSTD_FOUND)
            EXECUTE_PROCESS(COMMAND brew --prefix zstd OUTPUT_VARIABLE BREW_ZSTD_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
            set(ENV{ZSTD_HOME} ${BREW_ZSTD_DIR})
            set(ZSTD_HOME ${BREW_ZSTD_DIR})
            message(STATUS "Found locally installed zstd under $ENV{ZSTD_HOME}")
            # set variables
            file (TO_CMAKE_PATH "${ZSTD_HOME}" _zstd_path)
            find_library (ZSTD_LIBRARY NAMES zstd HINTS
                    ${_zstd_path}
                    PATH_SUFFIXES "lib" "lib64")
            if(ZSTD_LIBRARY)
                message(STATUS "zstd lib: ${ZSTD_LIBRARY}")
            endif()
            find_library (ZSTD_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}${ZSTD_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
                    ${_zstd_path}
                    PATH_SUFFIXES "lib" "lib64")
            if(ZSTD_LIBRARY)
                set(ZSTD_LIBRARIES "${ZSTD_LIBRARY}")
            elseif(ZSTD_STATIC_LIB)
                set(ZSTD_LIBRARIES "${ZSTD_STATIC_LIB}")
            endif()
            message(STATUS "Zstd libraries: ${ZSTD_LIBRARIES}")
            set(BUILD_AND_DOWNLOAD_ZLIB False)
        endif()
    endif()

    if(NOT ZSTD_LIBRARIES)
        message(STATUS "Building Zstd locally as 3rd party dependency.")
        set(ZSTD_HOME "${EXTERNAL_INSTALL_LOCATION}")
        set(ZSTD_INCLUDE_DIR "${ZSTD_HOME}/include")
        set(ZSTD_STATIC_LIB "${ZSTD_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}zstd${CMAKE_STATIC_LIBRARY_SUFFIX}")
        set(ZSTD_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ZSTD_HOME}
                -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_LIBDIR=lib -DZSTD_BUILD_TESTS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON)

        if (CMAKE_VERSION VERSION_GREATER "3.7")
            set(ZSTD_CONFIGURE SOURCE_SUBDIR "build/cmake" CMAKE_ARGS ${ZSTD_CMAKE_ARGS})
        else()
            set(ZSTD_CONFIGURE CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}" ${ZSTD_CMAKE_ARGS}
                    "${CMAKE_CURRENT_BINARY_DIR}/zstd_ep-prefix/src/zstd_ep/build/cmake")
        endif()

        ExternalProject_Add (zstd_ep
                URL "https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz"
                ${ZSTD_CONFIGURE}
                BUILD_BYPRODUCTS "${ZSTD_STATIC_LIB}"
                DOWNLOAD_EXTRACT_TIMESTAMP TRUE)

        set(ZSTD_LIBRARIES ${ZSTD_STATIC_LIB})

        add_library(zstd INTERFACE)
        target_link_libraries(zstd INTERFACE ${ZSTD_STATIC_LIB})
        target_include_directories(zstd SYSTEM INTERFACE ${ZSTD_INCLUDE_DIR})

        add_dependencies(zstd zstd_ep)
        install(FILES "${ZSTD_STATIC_LIB}" DESTINATION "lib")
        set(ZSTD_DEPENDS "zstd_ep")
    endif()
endif()

# following code is from https://github.com/OPM/opm-common/blob/master/cmake/Modules/UseSystemInfo.cmake
# read property from the newer /etc/os-release
function (read_release valuename FROM filename INTO varname)
    file (STRINGS ${filename} _distrib
            REGEX "^${valuename}="
            )
    string (REGEX REPLACE
            "^${valuename}=\"?\(.*\)" "\\1" ${varname} "${_distrib}"
            )
    # remove trailing quote that got globbed by the wildcard (greedy match)
    string (REGEX REPLACE
            "\"$" "" ${varname} "${${varname}}"
            )
    set (${varname} "${${varname}}" PARENT_SCOPE)
endfunction (read_release valuename FROM filename INTO varname)

# AWS Lambda backend, only buildable on Linux
if(UNIX AND NOT APPLE)
    set(LINUX TRUE)

    # check whether NAME in /etc/os-release is Amazon Linux AMI
    if(EXISTS /etc/os-release)
        read_release(NAME FROM /etc/os-release INTO RELEASE_NAME)
        if(RELEASE_NAME STREQUAL "Amazon Linux AMI")
            set(AMAZON_LINUX TRUE)
            message(INFO " Building on Amazon Linux")
        endif()
    endif()
endif()

# ncurses/curses lib for terminal manipulation
find_package(Curses REQUIRED)

# For debug tracing, actually link & include symbols (for macos right now only)
if(APPLE)
    # Use explicit stracktrace to produce errors
    include(FetchContent)

    # for this also requires libdwarf
    find_package(LibElf REQUIRED)
    find_package(LibDwarf REQUIRED)

    # Also requires one of: libbfd (gnu binutils), libdwarf, libdw (elfutils)
    FetchContent_Declare(backward
            GIT_REPOSITORY https://github.com/bombela/backward-cpp
            GIT_TAG master  # or a version tag, such as v1.6
            SYSTEM          # optional, the Backward include directory will be treated as system directory
            )
    FetchContent_MakeAvailable(backward)
    # Add Backward to your target (either Backward::Interface, Backward::Object, or Backward::Backward)
    #target_link_libraries(mytarget PUBLIC Backward::Interface)
endif()

# add subdirs here...
add_subdirectory(io)  # <-- make sure to call this first, because it changes parent scope with io dependencies
add_subdirectory(utils)
add_subdirectory(codegen)
add_subdirectory(core)
add_subdirectory(python)
add_subdirectory(runtime)
add_subdirectory(adapters)
# can only build aws lambda on linux platform
if(LINUX AND BUILD_WITH_AWS)
    # removed AWS lambda implementation, can be found on separate branch
    add_subdirectory(awslambda)
endif()
# call test dir last to get vars from before
add_subdirectory(test)



###########################################################################
# (7) Additional flags
###########################################################################
# additional flags for platforms
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
#    disable ranlib warning for no symbols on mac os x
    SET(CMAKE_C_ARCHIVE_CREATE   "<CMAKE_AR> Scr <TARGET> <LINK_FLAGS> <OBJECTS>")
    SET(CMAKE_CXX_ARCHIVE_CREATE "<CMAKE_AR> Scr <TARGET> <LINK_FLAGS> <OBJECTS>")
    SET(CMAKE_C_ARCHIVE_FINISH   "<CMAKE_RANLIB> -q -no_warning_for_no_symbols -c <TARGET>")
    SET(CMAKE_CXX_ARCHIVE_FINISH "<CMAKE_RANLIB> -q -no_warning_for_no_symbols -c <TARGET>")
endif()


# use gold linker for gcc
if(USE_LD_GOLD AND "${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
    execute_process(COMMAND ${CMAKE_C_COMPILER} -fuse-ld=gold -Wl,--version OUTPUT_VARIABLE stdout ERROR_QUIET)
    if("${stdout}" MATCHES "GNU gold")
        ucm_add_flags(-fuse-ld=gold)
        ucm_add_linker_flags(-fuse-ld=gold)
    else()
        message(WARNING "GNU gold linker isn't available, using the default system linker.")
    endif()
endif()

# enable rtti and exceptions
ucm_add_flags("-fexceptions -frtti")

# print flags
ucm_print_flags()


# TODO: check cloudpickle versions
# should be < 2.0.0 for python3.9 and >= 2.1.0 for python3.10
# python3 -c 'import cloudpickle; print(cloudpickle.__version__)'