# Create CMake project
cmake_minimum_required(VERSION 3.12 FATAL_ERROR)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(LAMBDA_NAME test-python)

# Create AWS cpp Project
find_package(aws-lambda-runtime)
add_executable(${LAMBDA_NAME} "main.cpp" "lambda_main.cc" "sighandler.cc")
target_link_libraries(${LAMBDA_NAME} PRIVATE AWS::aws-lambda-runtime ${AWSSDK_LINK_LIBRARIES})

# Link Python libraries + interpreter
set(PYTHON_RESOURCES_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../python38_resources)
target_include_directories(${LAMBDA_NAME} PRIVATE /usr/include/python3.8/)

# link tuplex - use the libraries from awslambda
target_link_libraries(${LAMBDA_NAME} PRIVATE libio libutils libcore ${Boost_LIBRARIES} ${LibMagic_LIBRARIES})

# Set compile flags/features
target_compile_features(${LAMBDA_NAME} PRIVATE "cxx_std_11")
# Uncomment if you want full warning output
#target_compile_options(${LAMBDA_NAME} PRIVATE "-Wall" "-Wextra")
add_dependencies(${LAMBDA_NAME} libio libutils libcore)
# this line creates a target that packages your binary and zips it up
aws_lambda_package_target(${LAMBDA_NAME} NO_LIBC)

# Add all the python dependencies to the zip file
add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME} POST_BUILD COMMAND cp -r ${PYTHON_RESOURCES_DIR}/bin . && zip -ur ${LAMBDA_NAME}.zip bin/)
add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME}
        POST_BUILD COMMAND cp -r ${PYTHON_RESOURCES_DIR}/lib .
        && cp -r ${PYTHON_RESOURCES_DIR}/usr_lib/* lib/python3.8/site-packages/
        && cp ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/*runtime* lib/
        && zip -ur ${LAMBDA_NAME}.zip lib/)
add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME} POST_BUILD COMMAND cp -r ${PYTHON_RESOURCES_DIR}/lib64 . && zip -ur ${LAMBDA_NAME}.zip lib64/)
