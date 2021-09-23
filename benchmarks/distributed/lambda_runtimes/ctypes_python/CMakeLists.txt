# Create CMake project
cmake_minimum_required(VERSION 3.9)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#project(test-python LANGUAGES CXX)
set(LAMBDA_NAME ctypes-tuplex)

# create so
add_library(${LAMBDA_NAME} SHARED "main.cpp" "lambda_main.cc" "sighandler.cc")
target_link_libraries(${LAMBDA_NAME} PRIVATE ${AWSSDK_LINK_LIBRARIES})

# Link Python libraries + interpreter
##set(PYTHON_RESOURCES_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../python_resources)
##target_include_directories(${LAMBDA_NAME} PRIVATE ${PYTHON_RESOURCES_DIR}/include/python3.8/)
#target_include_directories(${LAMBDA_NAME} PRIVATE /usr/include/python3.8/)

#find_library(LAMBDA_PYTHON38_PACKAGE NAMES python3.8) #PATHS ${PYTHON_RESOURCES_DIR}/libpy38 REQUIRED)
#message("(ctypes_python) Found Lambda Python package: ${LAMBDA_PYTHON38_PACKAGE}")
##link_directories(${CMAKE_SOURCE_DIR}/lib)
#target_link_libraries(${LAMBDA_NAME} PRIVATE ${LAMBDA_PYTHON38_PACKAGE})
target_link_libraries(${LAMBDA_NAME} PRIVATE gcc_s gcc)

# link tuplex - use the libraries from awslambda
target_link_libraries(${LAMBDA_NAME} PRIVATE libio libutils libcore ${Boost_LIBRARIES} ${LibMagic_LIBRARIES})

# Set compile flags/features
target_compile_features(${LAMBDA_NAME} PRIVATE "cxx_std_11")
#target_compile_options(${LAMBDA_NAME} PRIVATE "-Wall" "-Wextra")
add_dependencies(${LAMBDA_NAME} libio libutils libcore)
