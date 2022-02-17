CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# name of the lambda function
set(LAMBDA_NAME tplxlam)

# enable c++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# enable c11
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# only compiles on Linux, check here
if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    message(FATAL_ERROR "AWS cpp runtime only builds on Linux, therefore can build lambda executor only on Linux.")
endif()

find_package(aws-lambda-runtime)

#set(CMAKE_EXE_LINKER_FLAGS "-Wl,--copy-dt-needed-entries")
# cf. https://gitlab.kitware.com/cmake/cmake/issues/17297, fix for boost
#set(_Boost_STACKTRACE_BASIC_HEADERS     "boost/stacktrace.hpp")
#set(_Boost_STACKTRACE_BACKTRACE_HEADERS "boost/stacktrace.hpp")
#set(_Boost_STACKTRACE_ADDR2LINE_HEADERS "boost/stacktrace.hpp")
#set(_Boost_STACKTRACE_NOOP_HEADERS      "boost/stacktrace.hpp")
#
#find_package(Boost 1.66.0 COMPONENTS stacktrace_basic stacktrace_backtrace stacktrace_addr2line stacktrace_noop)
#
#find_package(Boost 1.66.0 COMPONENTS iostreams filesystem stacktrace REQUIRED)

# add nlohmann json
include(ExternalProject)
ExternalProject_Get_Property(json source_dir)
set(json_INCLUDE_DIR ${source_dir}/include)
include_directories(${json_INCLUDE_DIR})

# manually add the worker files to build AWS Lambda version...
include_directories("../worker/include")

# linking
add_executable(${LAMBDA_NAME} "src/main.cc"
        "src/lambda_main.cc"
        "src/sighandler.cc"
        "${CMAKE_CURRENT_LIST_DIR}/../worker/src/LambdaWorkerApp.cc"
        "${CMAKE_CURRENT_LIST_DIR}/../worker/src/WorkerApp.cc")

# enable export symbols in debug for backtrace
set_target_properties(${LAMBDA_NAME} PROPERTIES ENABLE_EXPORTS 1)
message(STATUS "Protobuf libs are: ${Protobuf_LIBRARIES}")

# note: for the Lambda, the interpreter gets actually embedded. Therefore link to it!
target_link_libraries(${LAMBDA_NAME} PRIVATE AWS::aws-lambda-runtime
        libio libutils libcore libcodegen libcpythonadapter
        ${AWSSDK_LINK_LIBRARIES} ${Boost_LIBRARIES} ${LibMagic_LIBRARIES} ${Protobuf_LIBRARIES} ${Python3_LIBRARIES})
target_compile_features(${LAMBDA_NAME} PRIVATE "cxx_std_11")


# @TODO: in order to reduce size of Lambda, strip JITCompiler away from codegen.
# it bloats the size.
# depend on nlohmann json
add_dependencies(${LAMBDA_NAME} json libio libutils libcore)

# this line creates a target that packages your binary and zips it up
if(AMAZON_LINUX)
    # when compiled on Amazon Linux, no need to package libc. reduces size.
    aws_lambda_package_target(${LAMBDA_NAME} NO_LIBC)
else()
    # need to package full libc
    aws_lambda_package_target(${LAMBDA_NAME})
endif()

# Add all the python dependencies to the zip file
set(PYTHON_RESOURCES_LOC ${CMAKE_CURRENT_SOURCE_DIR}/python38_resources)
set(PYTHON_RESOURCES_ZIP ${PYTHON_RESOURCES_LOC}.zip)
message("PYTHON_RESOURCES_ZIP = ${PYTHON_RESOURCES_ZIP}")
message("PYTHON_RESOURCES_LOC = ${PYTHON_RESOURCES_LOC}")

# To build Lambda runner deployment package, use ./scripts/create_lambda.zip.sh
