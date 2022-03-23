# (c) 2017 Leonhard Spiegelberg
# this build file builds the core component of the Tuplex project
CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(YAMLCPP REQUIRED)

# building with AWS backend support?
if(BUILD_WITH_AWS)
    # locate aws sdk & include lambda component
    find_package(AWSSDK REQUIRED COMPONENTS core s3 lambda)
    MESSAGE(STATUS "building with AWS Lambda backend")

    # communication with AWS Lambda happens via protobuf, i.e. make sure protobuf compiler
    # is installed
    set(Protobuf_USE_STATIC_LIBS ON)
    find_package(Protobuf REQUIRED)
    include_directories(Protobuf_INCLUDE_DIRS)
    protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS proto/Lambda.proto)
    message(STATUS "protobuf sources: ${PROTO_SRCS}")
    message(STATUS "protobuf headers: ${PROTO_HDRS}")
endif()


# CURL:
# Note: AWS SDK is only compatible with curl build against OpenSSL. Check this here!
# on linux, use ldd -v $(which curl) | grep OPENSSL which should yield a result.
find_package(CURL REQUIRED)
if(LINUX)
    message(STATUS "@TODO: check that curl was build against openssl")
    # ldd -v /usr/lib64/libcurl.so.4 | grep '(NSS'

    message(STATUS "CURL libraries: ${CURL_LIBRARIES}")
    message(STATUS "CURL include dirs: ${CURL_INCLUDE_DIR}")

    # this here should NOT yield any lines...!
    # ldd -v /usr/lib64/libcurl.so.4 | grep '(NSS'
endif()

include_directories("include")
include_directories(${Boost_INCLUDE_DIR})

# Source code & linking
file(GLOB_RECURSE SOURCES src/*.cc)

if(BUILD_WITH_AWS)
    # add protobuf srcs
    list(APPEND SOURCES ${PROTO_SRCS} ${PROTO_HDRS})
endif()

# can be only static because of llvms
add_library(libcore STATIC
        ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES})
set_target_properties(libcore PROPERTIES PREFIX "")

add_dependencies(libcore libcodegen libio libcpythonadapter runtime)

# Specify here the include directories exported
# by this library
target_include_directories(libcore PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR}
        ${YAMLCPP_INCLUDE_DIR}
        ${date_INCLUDE_DIR}
        ${CURL_INCLUDE_DIR}
        ${Python3_INCLUDE_DIRS}
        )

message(STATUS "Boost libraries are: ${Boost_LIBRARIES}")

message(STATUS "${LLVM_INCLUDE_DIRS}")
message(STATUS "${LLVM_DEFINITIONS}")
include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

# core has the runtime llvm libraries
llvm_map_components_to_libnames(core_llvm_libs nativecodegen scalaropts objcarcopts passes orcJIT orcshared orctargetprocess)
message(STATUS "${core_llvm_libs}")

# Declare the library
target_link_libraries(libcore
        libcodegen
        libio
        libcpythonadapter
        ${core_llvm_libs}
        ${YAMLCPP_LIBRARY}
        ${CURL_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Protobuf_LIBRARIES}
        Boost::iostreams
        Boost::thread
        Boost::system
        Boost::filesystem
        )