CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 14)

file(GLOB SRCS *.cc)
file(GLOB STDLIB_SRCS stdlib/*.cc)


include(GoogleTest)

add_executable(testcore ${SRCS} ${STDLIB_SRCS})
target_link_libraries(testcore libio
        libcodegen
        libcore
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES})

gtest_add_tests(TARGET testcore TEST_PREFIX "")