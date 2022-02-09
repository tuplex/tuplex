# (c) 2017 Leonhard Spiegelberg
# this build file is aimed to build the python package
CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# use pybind11 (header only library) to create python C-extension representing tuplex

# enable c++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# how should the module be named?
SET(MODULE_NAME tuplex)

# fetch pybind11 (external project)
include(FetchContent)
FetchContent_Declare(pybind11 GIT_REPOSITORY https://github.com/pybind/pybind11
                              GIT_TAG        v2.9.1)
FetchContent_GetProperties(pybind11)
if(NOT pybind11_POPULATED)
    FetchContent_Populate(pybind11)
    add_subdirectory(${pybind11_SOURCE_DIR} ${pybind11_BINARY_DIR})
endif()

# Source code & linking
file(GLOB_RECURSE SOURCES src/*.cc)

# this is the syntax for the old deprecated packages, switching out to newer one.
#PYTHON_ADD_MODULE(${MODULE_NAME} ${SOURCES})
message(STATUS "libs: ${Python3_LIBRARIES}")
message(STATUS "includes: ${Python3_INCLUDE_DIRS}")

pybind11_add_module(${MODULE_NAME} SHARED ${SOURCES})
set(PYMODULE "PYBIND11_MODULE(${MODULE_NAME}, m)")
target_compile_definitions(${MODULE_NAME} PRIVATE PYMODULE=${PYMODULE})
add_dependencies(${MODULE_NAME} libcore libcodegen)
target_include_directories(${MODULE_NAME} PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(${MODULE_NAME} PRIVATE pybind11::module
        libcodegen
        libcore
        libcpythonadapter
        Boost::iostreams
        Boost::thread
        Boost::system
        Boost::filesystem)


##Python3_add_library(${MODULE_NAME} MODULE ${SOURCES})
## because FindPython3 is broken in recent cmake, we can simply use add_library to define this directly
#add_library(${MODULE_NAME} MODULE ${SOURCES})
#set_target_properties(${MODULE_NAME} PROPERTIES PREFIX "")
#
## python pip package creation/distribution
#SET(PYTHON_DIST_DIR ${DIST_DIR}/python)
#
#message(STATUS "${MODULE_NAME}.so object will be saved to ${PYTHON_DIST_DIR}/tuplex/libexec")
#
## C++ shared object is placed in libexec within module
#SET_TARGET_PROPERTIES(${MODULE_NAME}
#        PROPERTIES
#        ARCHIVE_OUTPUT_DIRECTORY "${PYTHON_DIST_DIR}/tuplex/libexec"
#        LIBRARY_OUTPUT_DIRECTORY "${PYTHON_DIST_DIR}/tuplex/libexec"
#        RUNTIME_OUTPUT_DIRECTORY "${PYTHON_DIST_DIR}/tuplex/libexec"
#        )
#SET_TARGET_PROPERTIES(${MODULE_NAME}
#        PROPERTIES
#        ARCHIVE_OUTPUT_DIRECTORY_DEBUG "${PYTHON_DIST_DIR}/tuplex/libexec"
#        LIBRARY_OUTPUT_DIRECTORY_DEBUG "${PYTHON_DIST_DIR}/tuplex/libexec"
#        RUNTIME_OUTPUT_DIRECTORY_DEBUG "${PYTHON_DIST_DIR}/tuplex/libexec"
#        )
#SET_TARGET_PROPERTIES(${MODULE_NAME}
#        PROPERTIES
#        ARCHIVE_OUTPUT_DIRECTORY_RELEASE "${PYTHON_DIST_DIR}/tuplex/libexec"
#        LIBRARY_OUTPUT_DIRECTORY_RELEASE "${PYTHON_DIST_DIR}/tuplex/libexec"
#        RUNTIME_OUTPUT_DIRECTORY_RELEASE "${PYTHON_DIST_DIR}/tuplex/libexec"
#        )
#SET_TARGET_PROPERTIES(${MODULE_NAME}
#        PROPERTIES
#        ARCHIVE_OUTPUT_DIRECTORY_MINSIZEREL "${PYTHON_DIST_DIR}/tuplex/libexec"
#        LIBRARY_OUTPUT_DIRECTORY_MINSIZEREL "${PYTHON_DIST_DIR}/tuplex/libexec"
#        RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL "${PYTHON_DIST_DIR}/tuplex/libexec"
#        )
#SET_TARGET_PROPERTIES(${MODULE_NAME}
#        PROPERTIES
#        ARCHIVE_OUTPUT_DIRECTORY_RELWITHDEBINFO "${PYTHON_DIST_DIR}/tuplex/libexec"
#        LIBRARY_OUTPUT_DIRECTORY_RELWITHDEBINFO "${PYTHON_DIST_DIR}/tuplex/libexec"
#        RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO "${PYTHON_DIST_DIR}/tuplex/libexec"
#        )
#
#
#add_dependencies(${MODULE_NAME} libcore libcodegen)
#
#
## Specify here the include directories exported
## by this library
#target_include_directories(${MODULE_NAME} PUBLIC
#        ${CMAKE_CURRENT_SOURCE_DIR}/include
#        ${CMAKE_CURRENT_BINARY_DIR}
#        )
#
## add via define the module name back to C++
#SET(PYMODULE "BOOST_PYTHON_MODULE(${MODULE_NAME})")
#target_compile_definitions(${MODULE_NAME} PRIVATE PYMODULE=${PYMODULE})
#
## Declare the library
#target_link_libraries(${MODULE_NAME}
#        libcodegen
#        libcore
#        libcpythonadapter
#        Boost::iostreams
#        Boost::thread
#        Boost::system
#        Boost::filesystem)
## Because of Mac OS X, weakly link python libs
##target_link_libraries_with_dynamic_lookup(${MODULE_NAME} ${Python3_LIBRARIES} Boost::python)
#
##check if single generator or multiple
## copy setup.py/MANIFEST.in files and the directory tuplex
#FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/__init__.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/dataset.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/context.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/metrics.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/exceptions.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/distributed.py
#        DESTINATION ${PYTHON_DIST_DIR}/tuplex)
#
#FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/__init__.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/common.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/errors.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/interactive_shell.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/jedi_completer.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/jupyter.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/reflection.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/source_vault.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/framework.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/tracebacks.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/version.py
#        ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/utils/globs.py
#        DESTINATION ${PYTHON_DIST_DIR}/tuplex/utils)
#
#FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_tuples.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_arithmetic.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_metrics.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_logical.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_math.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_csv.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_index.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_filter.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_multi.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_fallback.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_config.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_parallelize.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_reflection.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_strings.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_dictionaries.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_lists.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_columns.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_nulls.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/notebook_utils.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_inspect.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_resolve.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_closure.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_import.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_math.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_aggregates.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_webui.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_exceptions.py
#          ${CMAKE_CURRENT_SOURCE_DIR}/tests/helper.py
#        DESTINATION ${PYTHON_DIST_DIR}/tests)
#FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/libexec/__init__.py DESTINATION ${PYTHON_DIST_DIR}/tuplex/libexec)
#FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/tuplex/repl/__init__.py
#          DESTINATION ${PYTHON_DIST_DIR}/tuplex/repl)
#FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/setup.py DESTINATION ${PYTHON_DIST_DIR})
#FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/MANIFEST.in DESTINATION ${PYTHON_DIST_DIR})
