# (c) 2018 Andy Ly
# this build file builds the adapters of the Tuplex project
CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

INCLUDE_DIRECTORIES("include")

INCLUDE_DIRECTORIES("${Python3_INCLUDE_DIRS}")

# Source code & linking
file(GLOB SOURCES src/*.cc)

add_library(libcpythonadapter OBJECT ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES})
add_dependencies(libcpythonadapter libutils)

# Specify here the include directories exported
# by this library
target_include_directories(libcpythonadapter PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR}
        ${Python3_INCLUDE_DIRS}
        )

# Declare the library
target_link_libraries(libcpythonadapter libutils)

## Because of Mac OS X, weakly link python libs
#target_link_libraries_with_dynamic_lookup(libcpythonadapter ${Python3_LIBRARIES})