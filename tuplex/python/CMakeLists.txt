# (c) 2017 Leonhard Spiegelberg
# this build file is aimed to build the python package
CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# use pybind11 (header only library) to create python C-extension representing tuplex

# enable c++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# how should the module be named?
SET(MODULE_NAME tuplex)

# deprecated
#set(PYBIND11_NOPYTHON ON CACHE INTERNAL "") 
message(STATUS "Pybind11 uses python version ${Python3_VERSION}")
set(PYBIND11_FINDPYTHON OFF CACHE INTERNAL "") 
set(PYBIND11_PYTHON_VERSION "${Python3_VERSION}" CACHE INTERNAL "")

# 3.0.0 requires python 3.10+, 2.13.6 supports python 3.9.
if(Python3_VERSION VERSION_LESS "3.10")
    set(pybind11_version v2.13.6)
else()
    set(pybind11_version v3.0.1)
endif()

FetchContent_Declare(pybind11 GIT_REPOSITORY https://github.com/pybind/pybind11
                             GIT_TAG        ${pybind11_version})
FetchContent_GetProperties(pybind11)
if(NOT pybind11_POPULATED)
    # Disable cmake warning, need to update to FetchContent_MakeAvailable eventually.
    cmake_policy(SET CMP0169 OLD)
    FetchContent_Populate(pybind11)
    cmake_policy(SET CMP0169 NEW)
    add_subdirectory(${pybind11_SOURCE_DIR} ${pybind11_BINARY_DIR})
endif()

# this is the syntax for the old deprecated packages, switching out to newer one.
#PYTHON_ADD_MODULE(${MODULE_NAME} ${SOURCES})
message(STATUS "libs: ${Python3_LIBRARIES}")
message(STATUS "includes: ${Python3_INCLUDE_DIRS}")

## use e.g. cpm https://github.com/cpm-cmake/CPM.cmake
## fetch pybind11 (external project)
#CPMAddPackage(
#    NAME pybind11
#    VERSION 2.9.1
#    GITHUB_REPOSITORY pybind/pybind11
#    OPTIONS
#        "PYBIND11_NOPYTHON ON"
#        "PYBIND11_FINDPYTHON OFF"
#)
###add_subdirectory(${pybind11_SOURCE_DIR} ${pybind11_BINARY_DIR})
#if(pybind11_ADDED)
#	message(STATUS "cpm added pybin11 ${pybind11_SOURCE_DIR}")
#else()
#	message(STATUS "not added ${pybind11_SOURCE_DIR}")
#endif()
#include(${pybind11_SOURCE_DIR}/tools/pybind11Common.cmake)
#include(${pybind11_SOURCE_DIR}/tools/pybind11Tools.cmake)
#include(${pybind11_SOURCE_DIR}/tools/pybind11NewTools.cmake)

# Method 1:
# cf. https://pybind11.readthedocs.io/en/stable/cmake/index.html
pybind11_add_module(${MODULE_NAME} ${SOURCES})
set(PYMODULE "PYBIND11_MODULE(${MODULE_NAME}, m)")
target_compile_definitions(${MODULE_NAME} PRIVATE PYMODULE=${PYMODULE})

# Macro/function to detect musl libc or Alpine Linux
function(detect_musl OUT_VAR)
    include(CheckCXXSourceCompiles)
    set(_musl_detected OFF)

    # Method 1: Check for musl-specific functions that don't exist in glibc
    check_cxx_source_compiles("
        #include <stdio.h>
        #include <stdlib.h>
        int main() {
            // musl has __progname, glibc doesn't
            extern char *__progname;
            return 0;
        }
    " _musl_progname_detected)

    if(_musl_progname_detected)
        set(_musl_detected ON)
    else()
        # Method 2: Check for musl-specific header behavior
        check_cxx_source_compiles("
            #include <sys/types.h>
            #include <sys/socket.h>
            int main() {
                // musl defines MSG_NOSIGNAL, glibc doesn't always
                int flag = MSG_NOSIGNAL;
                return 0;
            }
        " _musl_socket_detected)

        if(_musl_socket_detected)
            set(_musl_detected ON)
        else()
            # Method 3: Check for Alpine Linux specific behavior
            check_cxx_source_compiles("
                #include <features.h>
                int main() {
                    // Check if we're on Alpine/musl by looking for specific defines
                    #ifdef __GLIBC__
                    #error glibc detected
                    #endif
                    return 0;
                }
            " _musl_features_detected)

            if(_musl_features_detected)
                set(_musl_detected ON)
            endif()
        endif()
    endif()

    if(APPLE)
        set(_musl_detected OFF)
    endif()

    set(${OUT_VAR} ${_musl_detected} PARENT_SCOPE)
endfunction()

# Detect whether muslc is being used.
detect_musl(MUSL_DETECTED)

if(MUSL_DETECTED)
    message(STATUS "muslc detected, adjusting linker flags")

    # # Option 1:
    # # There's something going wrong on Alpine, all the symbols get optimized away. Therefore manually disable for target.
    # message(STATUS "Detected musl libc or Alpine Linux, disabling interprocedural optimization for ${MODULE_NAME}")
    # set_target_properties(${MODULE_NAME} PROPERTIES
    #         INTERPROCEDURAL_OPTIMIZATION OFF
    #         CXX_VISIBILITY_PRESET hidden
    #         VISIBILITY_INLINES_HIDDEN ON)

    # Option 2:
    # Pass the right linker flags for Alpine. In addition, need to have a list of symbols to preserve as
    # they need to be present when using the C-extension module.
    target_link_options(${MODULE_NAME} PRIVATE
            "-Wl,--export-dynamic"
            "-Wl,--retain-symbols-file=${CMAKE_CURRENT_SOURCE_DIR}/preserve_symbols.txt"
            "-Wl,--allow-shlib-undefined")
endif()

add_dependencies(${MODULE_NAME} libcore libcodegen)
target_include_directories(${MODULE_NAME} PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR})
if(APPLE)
target_link_libraries(${MODULE_NAME} PUBLIC Backward::Interface
        PRIVATE
        libcodegen
        libcore
        libcpythonadapter
        libutils
        libio
        Boost::iostreams
        Boost::thread
        Boost::filesystem)
else()
    target_link_libraries(${MODULE_NAME}
            PRIVATE
            libcodegen
            libcore
            libcpythonadapter
            libutils
            libio
            Boost::iostreams
            Boost::thread
            Boost::filesystem)
endif()

#check if single generator or multiple
# copy setup.py/MANIFEST.in files and the directory tuplex
# python pip package creation/distribution
SET(PYTHON_DIST_DIR ${DIST_DIR}/python)
message(STATUS "${MODULE_NAME}.so object will be saved to ${PYTHON_DIST_DIR}/tuplex/libexec")
# C++ shared object is placed in libexec within module
SET_TARGET_PROPERTIES(${MODULE_NAME}
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${PYTHON_DIST_DIR}/tuplex/libexec"
        LIBRARY_OUTPUT_DIRECTORY "${PYTHON_DIST_DIR}/tuplex/libexec"
        RUNTIME_OUTPUT_DIRECTORY "${PYTHON_DIST_DIR}/tuplex/libexec"
        )
SET_TARGET_PROPERTIES(${MODULE_NAME}
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY_DEBUG "${PYTHON_DIST_DIR}/tuplex/libexec"
        LIBRARY_OUTPUT_DIRECTORY_DEBUG "${PYTHON_DIST_DIR}/tuplex/libexec"
        RUNTIME_OUTPUT_DIRECTORY_DEBUG "${PYTHON_DIST_DIR}/tuplex/libexec"
        )
SET_TARGET_PROPERTIES(${MODULE_NAME}
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY_RELEASE "${PYTHON_DIST_DIR}/tuplex/libexec"
        LIBRARY_OUTPUT_DIRECTORY_RELEASE "${PYTHON_DIST_DIR}/tuplex/libexec"
        RUNTIME_OUTPUT_DIRECTORY_RELEASE "${PYTHON_DIST_DIR}/tuplex/libexec"
        )
SET_TARGET_PROPERTIES(${MODULE_NAME}
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY_MINSIZEREL "${PYTHON_DIST_DIR}/tuplex/libexec"
        LIBRARY_OUTPUT_DIRECTORY_MINSIZEREL "${PYTHON_DIST_DIR}/tuplex/libexec"
        RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL "${PYTHON_DIST_DIR}/tuplex/libexec"
        )
SET_TARGET_PROPERTIES(${MODULE_NAME}
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY_RELWITHDEBINFO "${PYTHON_DIST_DIR}/tuplex/libexec"
        LIBRARY_OUTPUT_DIRECTORY_RELWITHDEBINFO "${PYTHON_DIST_DIR}/tuplex/libexec"
        RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO "${PYTHON_DIST_DIR}/tuplex/libexec"
        )
FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/__init__.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/dataset.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/context.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/metrics.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/exceptions.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/distributed.py
        DESTINATION ${PYTHON_DIST_DIR}/tuplex)

FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/__init__.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/common.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/errors.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/interactive_shell.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/jedi_completer.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/jupyter.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/reflection.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/source_vault.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/framework.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/tracebacks.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/version.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/globs.py
        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/dllist.py
        DESTINATION ${PYTHON_DIST_DIR}/tuplex/utils)

FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_tuples.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_arithmetic.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_metrics.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_logical.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_math.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_csv.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_index.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_filter.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_multi.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_fallback.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_config.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_parallelize.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_reflection.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_strings.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_dictionaries.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_lists.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_columns.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_nulls.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/notebook_utils.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_inspect.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_resolve.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_closure.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_import.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_math.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_aggregates.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_webui.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_exceptions.py
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/helper.py
        DESTINATION ${PYTHON_DIST_DIR}/tests)
FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/libexec/__init__.py DESTINATION ${PYTHON_DIST_DIR}/tuplex/libexec)
FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/repl/__init__.py
          DESTINATION ${PYTHON_DIST_DIR}/tuplex/repl)
FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/setup.py DESTINATION ${PYTHON_DIST_DIR})
FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/MANIFEST.in DESTINATION ${PYTHON_DIST_DIR})
