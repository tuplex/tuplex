# (c) 2018 Andy Ly
# this build file builds the tests for adapters of the Tuplex project
CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

## find python libs
#FIND_PACKAGE(PythonInterp 3 REQUIRED)
#FIND_PACKAGE(PythonLibs 3 REQUIRED)
#
#IF (PYTHONINTERP_FOUND)
#    MESSAGE("found python version ${PYTHON_VERSION_STRING}")
#    MESSAGE("using ${PYTHON_INCLUDE_DIRS}")
#    INCLUDE_DIRECTORIES("${PYTHON_INCLUDE_DIRS}")
#ENDIF ()

FILE(GLOB SRCS *.cc)

include(GoogleTest)

ADD_EXECUTABLE(testcpythonadapter ${SRCS})

# Note: the libcpythonadapter is already included in core/codegen. Do not double include b.c. it breaks dependencies :)
TARGET_LINK_LIBRARIES(testcpythonadapter
        libcpythonadapter
        ${GTest_LIBRARIES}
        libcore
        libcodegen
        libutils
        ${Python3_LIBRARIES}
        )

gtest_add_tests(TARGET testcpythonadapter TEST_PREFIX "")