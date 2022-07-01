CMAKE_MINIMUM_REQUIRED(VERSION 3.19 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 14)
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

IF(BREW_FOUND)
    IF(APPLE)

        # there might be multiple LLVM versions installed.
        # check which version there is
        # if not sys.stdin.isatty():
        #    data = sys.stdin.readlines()

        # could use brew prefix here, but let's leave it like this
        EXECUTE_PROCESS(COMMAND bash "-c" "brew info llvm | grep Cellar | cut -d ' ' -f 1" OUTPUT_VARIABLE LLVM_ROOT_DIR RESULT_VARIABLE BREW_LLVM_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
        IF(NOT BREW_LLVM_NOTFOUND EQUAL "0")
            MESSAGE(WARNING "did not find llvm, you might install it via `brew install llvm@9`")
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
    # make cmake find in config mode the right LLVMConfig.cmake file which is located here
    set(LLVM_DIR "${LLVM_ROOT_DIR}/lib/cmake/llvm")
    find_package(LLVM CONFIG REQUIRED) # find with whatever llvm version has been specified
ELSE()
    # try to search for LLVM9, then LLVM6
    find_package(LLVM CONFIG)
    if(NOT LLVM_FOUND)
	    find_package(LLVM 9 CONFIG REQUIRED)
    endif()
ENDIF()

MESSAGE(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
MESSAGE(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
MESSAGE(STATUS "Found LLVM include dirs at: " ${LLVM_INCLUDE_DIRS})
message(STATUS "LLVM shared link mode: ${LLVM_IS_SHARED}")
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
        ${LLVM_SYSTEM_LIBS}
        ${ZLIB_LIBRARIES})