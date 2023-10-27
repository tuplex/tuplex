CMAKE_MINIMUM_REQUIRED(VERSION 3.19 FATAL_ERROR)

# enable c++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

## external libraries

# LLVM
# list to reduce size of shared object. Compared to linking against all LLVM components, this saves about ~10MB.
# from https://github.com/llvm-mirror/llvm/blob/master/cmake/modules/LLVM-Config.cmake#L218?
# for minimum JIT these components are recommended:
#    core
#    executionengine
#    native
#    object
#    orcjit
#    runtimedyld
#    support
# this may make it easier but increases size of shared object tremendously
set(LLVM_REQUIRED_COMPONENTS core orcjit nativecodegen native scalaropts objcarcopts passes)

find_package(LLVM CONFIG REQUIRED)
if(LLVM_DIR)
    message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
endif()
MESSAGE(STATUS "Found LLVM include dirs at: " ${LLVM_INCLUDE_DIRS})
MESSAGE(STATUS "LLVM library dir: ${LLVM_LIBRARY_DIRS}")
set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} ${LLVM_LIBRARY_DIRS})
llvm_map_components_to_libnames(llvm_libs ${LLVM_REQUIRED_COMPONENTS})
set(LLVM_LIBRARIES "${llvm_libs}")
include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

# special case on Unix, make use rtti gets the right flag for the build
if (NOT LLVM_ENABLE_RTTI)
        message(WARNING
                "This build configuration is not supported and will likely not work."
                "You should recompile LLVM with RTTI enabled.")
endif()

# BOOST libs
include_directories(${Boost_INCLUDE_DIR})

# ANTLR4 Runtime (installed e.g. via brew install antlr4-cpp-runtime)
find_package(ANTLR4Runtime REQUIRED)

# Source code & linking
file(GLOB_RECURSE SOURCES src/*.cc)

# antlr target
add_definitions(-DANTLR4CPP_STATIC)
set(ANTLR4_WITH_STATIC_CRT OFF)
include(ExternalAntlr4Cpp)
include_directories(${ANTLR4_INCLUDE_DIRS})
set(ANTLR_EXECUTABLE ${CMAKE_CURRENT_SOURCE_DIR}/tools/antlr-4.13.1-complete.jar)
find_package(ANTLR ${ANTLR4Runtime_VERSION})

# if package fails, try to download proper antlr4 tool
if(NOT ANTLR_FOUND)
    set(ANTLR_TOOL_URL "https://www.antlr.org/download/antlr-${ANTLR4Runtime_VERSION}-complete.jar")
    message(STATUS "Downloading compatible ANTLR tool from ${ANTLR_TOOL_URL}")
    file(DOWNLOAD ${ANTLR_TOOL_URL} ${CMAKE_CURRENT_SOURCE_DIR}/tools/antlr-${ANTLR4Runtime_VERSION}-complete.jar SHOW_PROGRESS)
    set(ANTLR_EXECUTABLE ${CMAKE_CURRENT_SOURCE_DIR}/tools/antlr-${ANTLR4Runtime_VERSION}-complete.jar)
    # run again, this time in required mode
    find_package(ANTLR ${ANTLR4Runtime_VERSION} REQUIRED)
endif()


# check that antlr and antlr target version are compatible -> if not, abort.
message(STATUS "Antlr4 runtime version ${ANTLR4Runtime_VERSION}")
message(STATUS "Antlr4 version ${ANTLR4_VERSION}")

if(NOT ANTLR4Runtime_VERSION VERSION_EQUAL ANTLR4_VERSION)
    message(FATAL_ERROR "Antlr versions not compatible, runtime is ${ANTLR4Runtime_VERSION} but antlr tool is ${ANTLR4_VERSION}")
endif()

antlr_target(Python3Grammar ${CMAKE_CURRENT_SOURCE_DIR}/grammar/Python3.g4 OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/managed PACKAGE antlr4 LISTENER VISITOR)

# enable rtti and exceptions
ucm_add_flags("-fexceptions -frtti")

add_library(libcodegen OBJECT
        ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES} ${ANTLR_Python3Grammar_CXX_OUTPUTS})
set_target_properties(libcodegen PROPERTIES PREFIX ""
        LINK_FLAGS "${LLVM_LDFLAGS}")

# Specify here the include directories exported
# by this library
target_include_directories(libcodegen PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/managed
        ${CMAKE_CURRENT_BINARY_DIR}
        ${LLVM_INCLUDE_DIRS}
        ${Boost_INCLUDE_DIR}
        ${ANTLR4Runtime_INCLUDE_DIR}
        ${PCRE2_INCLUDE_DIRS}
        )

# Declare the library
target_link_libraries(libcodegen
        libutils
        ${FFI_LIBRARIES}
        ${ANTLR4Runtime_LIB}
        ${AWSSDK_LINK_LIBRARIES}
        ${PCRE2_LIBRARIES}
        ${LLVM_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${CURSES_LIBRARIES}
        fmt::fmt
        )