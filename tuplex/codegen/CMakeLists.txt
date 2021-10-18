CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

## external libraries

## check for ICU
#IF(BREW_FOUND)
#    IF(APPLE)
#        MESSAGE("brew on Mac found")
#        EXECUTE_PROCESS(COMMAND brew --prefix icu4c OUTPUT_VARIABLE ICU_ROOT_DIR ERROR_VARIABLE BREW_ICU_NOTFOUND)
#        IF(BREW_ICU_NOTFOUND)
#            MESSAGE("did not find brewed icu, you might install it via brew install icu4c")
#        ELSE()
#            MESSAGE("found brewed icu under: " ${ICU_ROOT_DIR})
#        ENDIF()
#
#    ELSEIF(UNIX)
#        MESSAGE("brew on Unix found")
#    ENDIF()
#ENDIF()


# add LLVM as external project
# note that building LLVM might take a while...
# However, this is the cleanest method to guarantee version compatibility

#SET(LLVM_URL "http://releases.llvm.org/5.0.0/llvm-5.0.0.src.tar.xz" CACHE STRING "llvm repo")
#include(ExternalProject)
#ExternalProject_Add(llvm PREFIX llvm URL ${LLVM_URL} BUILD_IN_SOURCE INSTALL_DIR ${CMAKE_BINARY_DIR}/install CMAKE_ARGS )

# using llvm via brew, easiest and avoid costly LLVM build (might take up to 20min)
# adding LLVM 9.0
IF(BREW_FOUND)
    IF(APPLE)

        # there might be multiple LLVM versions installed.
        # check which version there is
        # if not sys.stdin.isatty():
        #    data = sys.stdin.readlines()

        # could use brew prefix here, but let's leave it like this
        EXECUTE_PROCESS(COMMAND bash "-c" "brew info llvm | grep Cellar | cut -d ' ' -f 1" OUTPUT_VARIABLE LLVM_ROOT_DIR RESULT_VARIABLE BREW_LLVM_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
        IF(NOT BREW_LLVM_NOTFOUND EQUAL "0")
            MESSAGE("did not find llvm, you might install it via `brew install llvm@9`")
        ELSE()
            # check version, needs to be within 5 and 9 incl.
            # i.e. execute something like /usr/local/opt/llvm/bin/llvm-config --version
            EXECUTE_PROCESS(COMMAND ${LLVM_ROOT_DIR}/bin/llvm-config --version OUTPUT_VARIABLE LLVM_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)

            # check if empty, if it is parse again using brew info json
            IF("${LLVM_VERSION}" STREQUAL "")
                EXECUTE_PROCESS(COMMAND bash "-c" "brew info --json=v1 llvm | python3 -c 'import sys,json; x=json.load(sys.stdin); print(x[0][\"versions\"][\"stable\"])'" OUTPUT_VARIABLE LLVM_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
            ENDIF()

            IF("${LLVM_VERSION}" STREQUAL "" OR "${LLVM_VERSION}" VERSION_LESS 5.0.0 OR "${LLVM_VERSION}" VERSION_GREATER_EQUAL 10.0.0)
                # check if any other llvm version is installed...
                MESSAGE(STATUS "LLVM version installed is ${LLVM_VERSION}, which is incompatible with Tuplex")

                # check for llvm@9 (do not check other versions)
                # note that we can't simply use brew --prefix due to different subversions in brew...
                execute_process(COMMAND bash "-c" "brew info llvm@9 | grep Cellar | cut -d ' ' -f 1" OUTPUT_VARIABLE LLVM_ROOT_DIR RESULT_VARIABLE BREW_RET OUTPUT_STRIP_TRAILING_WHITESPACE)
                if(BREW_RET EQUAL "1")
                    message(FATAL_ERROR "checked whether in addition to ${LLVM_VERSION} LLVM 9.x is installed, but could not could find it. Please install via `brew install llvm@9`")
                else()
                    message(STATUS "Found another installed llvm version under ${LLVM_ROOT_DIR}, using this version for Tuplex.")
                endif()
            ELSE()
                MESSAGE(STATUS "found brewed llvm under: " ${LLVM_ROOT_DIR})
            ENDIF()
        ENDIF()

    ELSEIF(UNIX)
        # ...
    ENDIF()
ENDIF()

# for brewed llvm, add to cmakemodulepath
IF(LLVM_ROOT_DIR)
    message(STATUS "Given LLVM_ROOT_DIR=${LLVM_ROOT_DIR}")
    message(STATUS "CMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}")
    EXECUTE_PROCESS(COMMAND "ls" "/opt" COMMAND_ECHO STDOUT)
    EXECUTE_PROCESS(COMMAND "ls" "${LLVM_ROOT_DIR}" COMMAND_ECHO STDOUT)
    # make cmake find in config mode the right LLVMConfig.cmake file which is located here
    set(LLVM_DIR "${LLVM_ROOT_DIR}/lib/cmake/llvm")
ENDIF()

FIND_PACKAGE(LLVM REQUIRED CONFIG)
MESSAGE(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
MESSAGE(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
MESSAGE(STATUS "Found llvm include dirs at: " ${LLVM_INCLUDE_DIRS})
include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

# special case on Unix, make use rtti gets the right flag for the build
if (NOT LLVM_ENABLE_RTTI)
        message(WARNING
                "This build configuration is not supported and will likely not work."
                "You should recompile LLVM with RTTI enabled.")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")
endif()

## libffi
#find_package(FFI 3.2.1 REQUIRED)
#include_directories(${FFI_INCLUDE_DIRS})

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
set(ANTLR_EXECUTABLE ${CMAKE_CURRENT_SOURCE_DIR}/tools/antlr-4.8-complete.jar)
find_package(ANTLR REQUIRED)

antlr_target(Python3Grammar ${CMAKE_CURRENT_SOURCE_DIR}/grammar/Python3.g4 OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/managed PACKAGE antlr4 LISTENER VISITOR)



add_library(libcodegen STATIC
        ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES} ${ANTLR_Python3Grammar_CXX_OUTPUTS})

set_target_properties(libcodegen PROPERTIES PREFIX "")

# find libraries for LLVM components that are intended to be used
#llvm_map_components_to_libnames(llvm_libs support core irreader)
# to get list overview, use llvm-config --components

# list to reduce size of shared object. Compared to linking against all LLVM components, this saves about ~10MB.
llvm_map_components_to_libnames(llvm_libs core orcjit nativecodegen native scalaropts objcarcopts passes)

# for minimum JIT these components are recommended:
#    core
#    executionengine
#    native
#    object
#    orcjit
#    runtimedyld
#    support
# this may make it easier but increases size of shared object tremendously
#llvm_map_components_to_libnames(llvm_libs all)

#add_dependencies(libcodegen GeneratePython3Parser)

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
        ${llvm_libs}
        ${FFI_LIBRARIES}
        ${ANTLR4Runtime_LIB}
        ${AWSSDK_LINK_LIBRARIES}
        ${PCRE2_LIBRARIES})