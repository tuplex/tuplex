CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++17
set(CMAKE_CXX_STANDARD 17)

file(GLOB SRCS *.cc)

include(GoogleTest)

ADD_EXECUTABLE(testcodegen ${SRCS})

ASSERT_VAR(CURSES_LIBRARIES)

TARGET_LINK_LIBRARIES(testcodegen
        libcodegen
        ${GTest_LIBRARIES}
        ${ZSTD_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${CURSES_LIBRARIES}
        runtime
        )

gtest_add_tests(TARGET testcodegen TEST_PREFIX "")