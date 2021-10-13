# Create CMake project
cmake_minimum_required(VERSION 3.12)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#project(test-python LANGUAGES CXX)
set(LAMBDA_NAME ctypes-tuplex)

# create so
add_library(${LAMBDA_NAME} SHARED "main.cpp" "lambda_main.cc" "sighandler.cc")
target_link_libraries(${LAMBDA_NAME} PRIVATE ${AWSSDK_LINK_LIBRARIES})

target_link_libraries(${LAMBDA_NAME} PRIVATE gcc_s gcc)

# link tuplex - use the libraries from awslambda
target_link_libraries(${LAMBDA_NAME} PRIVATE libio libutils libcore ${Boost_LIBRARIES} ${LibMagic_LIBRARIES})

# Set compile flags/features
target_compile_features(${LAMBDA_NAME} PRIVATE "cxx_std_11")
# Uncomment for full warnings
#target_compile_options(${LAMBDA_NAME} PRIVATE "-Wall" "-Wextra")
add_dependencies(${LAMBDA_NAME} libio libutils libcore)
