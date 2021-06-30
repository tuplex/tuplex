# (c) 2017 Leonhard Spiegelberg
# this build file builds the core component of the Tuplex project
CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(YAMLCPP REQUIRED)

# fix for Mac OS X/ brew, find openSSL
## check for ICU
IF(BREW_FOUND)
    IF(APPLE)
        # MESSAGE("brew on Mac found")
        EXECUTE_PROCESS(COMMAND brew --prefix openssl OUTPUT_VARIABLE OSSL_ROOT_DIR ERROR_VARIABLE BREW_OPENSSL_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
        IF(BREW_OPENSSL_NOTFOUND)
            MESSAGE("did not find brewed openssl, you might install it via brew install openssl")
        ELSE()
            set(ENV{OPENSSL_ROOT_DIR} /usr/local/opt/openssl)
            MESSAGE(STATUS "found brewed openssl under: ${OSSL_ROOT_DIR}")
        ENDIF()

    ELSEIF(UNIX)
        #MESSAGE("brew on Unix found")
    ENDIF()
ENDIF()

set(CURL_LIBRARY "-lcurl")
find_package(CURL REQUIRED)


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

include_directories("include")
include_directories(${Boost_INCLUDE_DIR})

# Source code & linking
file(GLOB_RECURSE SOURCES src/*.cc)

if(BUILD_WITH_AWS)
    # add protobuf srcs
    list(APPEND SOURCES ${PROTO_SRCS} ${PROTO_HDRS})
endif()

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
        )

# Declare the library
target_link_libraries(libcore
        libcodegen
        libio
        libcpythonadapter
        ${YAMLCPP_LIBRARY}
        ${CURL_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Boost_LIBRARIES}
        ${Protobuf_LIBRARIES}
        )