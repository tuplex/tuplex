# (c) 2017 Leonhard Spiegelberg
# this build file builds the core component of the Tuplex project
CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(YAMLCPP REQUIRED)

# building with AWS backend support?
if(BUILD_WITH_AWS)
    # locate aws sdk & include lambda component
    find_package(AWSSDK REQUIRED COMPONENTS core s3 lambda)
    MESSAGE(STATUS "building with AWS Lambda backend")

    # communication with AWS Lambda happens via protobuf, i.e. make sure protobuf compiler
    # is installed
    # set(Protobuf_USE_STATIC_LIBS ON)
    # https://github.com/protocolbuffers/protobuf/issues/12637
    find_package(Protobuf CONFIG)
    if(NOT Protobuf_FOUND)
        find_package(Protobuf REQUIRED)
    endif()
    include_directories(Protobuf_INCLUDE_DIRS)

    # https://github.com/protocolbuffers/protobuf/blob/e1faf09604d26cc6803970815f91225b220175d4/docs/cmake_protobuf_generate.md
    # depending on version, use protobuf_generate_cpp or protobuf_generate
    if((Protobuf_VERSION VERSION_GREATER_EQUAL "3.22" AND Protobuf_VERSION VERSION_LESS "4.0") OR (Protobuf_VERSION VERSION_GREATER_EQUAL "4.3.22" AND Protobuf_VERSION VERSION_LESS "5.0.0") OR (Protobuf_VERSION VERSION_GREATER_EQUAL "22.0"))
        # see https://github.com/protocolbuffers/protobuf/blob/e1faf09604d26cc6803970815f91225b220175d4/docs/cmake_protobuf_generate.md
        add_library(proto-objects OBJECT "${CMAKE_CURRENT_LIST_DIR}/proto/Lambda.proto")
        target_link_libraries(proto-objects PUBLIC protobuf::libprotobuf)
        set(PROTO_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/managed")
        target_include_directories(proto-objects PUBLIC "$<BUILD_INTERFACE:${PROTO_BINARY_DIR}>")

        protobuf_generate(
                TARGET proto-objects
                IMPORT_DIRS "${CMAKE_CURRENT_LIST_DIR}/proto"
                PROTOC_OUT_DIR "${PROTO_BINARY_DIR}")

    else()
        protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS proto/Lambda.proto)
    endif()
    message(STATUS "protobuf sources (v${Protobuf_VERSION}): ${PROTO_SRCS}")
    message(STATUS "protobuf headers (v${Protobuf_VERSION}): ${PROTO_HDRS}")
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

add_library(libcore OBJECT
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

# make sure llvm dependencies exist
ASSERT_VAR(ZLIB_LIBRARIES)
ASSERT_VAR(ZSTD_LIBRARIES)

# Declare the library
target_link_libraries(libcore
        libcodegen
        libio
        libcpythonadapter
        ${YAMLCPP_LIBRARY}
        ${CURL_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Protobuf_LIBRARIES}
        Boost::iostreams
        Boost::thread
        Boost::system
        Boost::filesystem
	    util
        ${ZLIB_LIBRARIES}
        ${ZSTD_LIBRARIES}
        )
