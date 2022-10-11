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
if(UNIX AND NOT APPLE)
    message(STATUS "CURL libraries: ${CURL_LIBRARIES}")
    message(STATUS "CURL include dirs: ${CURL_INCLUDE_DIR}")

    execute_process(COMMAND bash "-c" "ldd -v ${CURL_LIBRARIES} | grep -o -E '\(OPENSSL.*\)'" OUTPUT_VARIABLE SSL_OUT RESULT_VARIABLE SSL_RET OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(SSL_RET AND NOT SSL_RET EQUAL 0)
	    message(FATAL_ERROR "failed to check that curl was built against openssl")
    else()
	    if(SSL_OUT AND SSL_OUT STREQUAL "")
		    execute_process(COMMAND bash "-c" "ldd -v ${CURL_LIBRARIES} | grep -o -E '\(.*SSL.*\)' | head -n1" OUTPUT_VARIABLE SSL_OUT RESULT_VARIABLE SSL_RET OUTPUT_STRIP_TRAILING_WHITESPACE)
		    message(FATAL_ERROR "libcurl was not built against OpenSSL, but against ${SSL_OUT}")
	    else()
		message(STATUS "Verified that curl was built against OpenSSL")
	    endif()
    endif()

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
add_library(libcore ${LIBRARY_MODE}
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
llvm_map_components_to_libnames(core_llvm_libs nativecodegen scalaropts objcarcopts passes orcJIT)
message(STATUS "llvm lib dirs: ${LLVM_LIBRARY_DIRS}")
link_directories(${LLVM_LIBRARY_DIRS})
message(STATUS "${core_llvm_libs}")

#target_include_directories(libcore PRIVATE libcodegen)

#llvm_config(libcore USE_SHARED ${core_llvm_libs})
# Declare the library
target_link_libraries(libcore
        PRIVATE
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
	util
        )

# precompiled headers, mostly to speed up cerealization of operators
if(USE_PRECOMPILED_HEADERS)
    # mainly operators but also include codegen context
    file(GLOB_RECURSE LOGICAL_OPERATORS_HEADER include/logical/*.h)
    set(CORE_HEADERS ${LOGICAL_OPERATORS_HEADER} include/physical/codegen/CodeGenerationContext.h include/UDF.h)
    target_precompile_headers(libcore PUBLIC ${CORE_HEADERS})
endif()
