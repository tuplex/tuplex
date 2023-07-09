CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++17
set(CMAKE_CXX_STANDARD 17)

file(GLOB SRCS *.cc)

include(GoogleTest)

ADD_EXECUTABLE(testcodegen ${SRCS})


# Zstd
EXECUTE_PROCESS(COMMAND brew list zstd OUTPUT_VARIABLE BREW_ZSTD_LIST ERROR_VARIABLE BREW_ZSTD_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
if(BREW_ZSTD_NOTFOUND)
    message(STATUS "Could not find locally installed zstd, building third party")
    set(ZSTD_VERSION "1.5.0")
    set(ZSTD_HOME "${EXTERNAL_INSTALL_LOCATION}")
    set(ZSTD_INCLUDE_DIR "${ZSTD_HOME}/include")
    set(ZSTD_STATIC_LIB "${ZSTD_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}zstd${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(ZSTD_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ZSTD_HOME}
            -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_LIBDIR=lib -DZSTD_BUILD_TESTS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON)

    if (CMAKE_VERSION VERSION_GREATER "3.7")
        set(ZSTD_CONFIGURE SOURCE_SUBDIR "build/cmake" CMAKE_ARGS ${ZSTD_CMAKE_ARGS})
    else()
        set(ZSTD_CONFIGURE CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}" ${ZSTD_CMAKE_ARGS}
                "${CMAKE_CURRENT_BINARY_DIR}/zstd_ep-prefix/src/zstd_ep/build/cmake")
    endif()

    ExternalProject_Add (zstd_ep
            URL "https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz"
            ${ZSTD_CONFIGURE}
            BUILD_BYPRODUCTS "${ZSTD_STATIC_LIB}")

    set(ZSTD_LIBRARIES ${ZSTD_STATIC_LIB})

    add_library(zstd INTERFACE)
    target_link_libraries(zstd INTERFACE ${ZSTD_STATIC_LIB})
    target_include_directories(zstd SYSTEM INTERFACE ${ZSTD_INCLUDE_DIR})

    add_dependencies(zstd zstd_ep)
    install(FILES "${ZSTD_STATIC_LIB}" DESTINATION "lib")
    set(ZSTD_DEPENDS "zstd_ep")
else()
    EXECUTE_PROCESS(COMMAND brew --prefix zstd OUTPUT_VARIABLE BREW_ZSTD_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(ENV{ZSTD_HOME} ${BREW_ZSTD_DIR})
    set(ZSTD_HOME ${BREW_ZSTD_DIR})
    message(STATUS "Found locally installed zstd under $ENV{ZSTD_HOME}")
    # set variables
    file (TO_CMAKE_PATH "${ZSTD_HOME}" _zstd_path)
    find_library (ZSTD_LIBRARY NAMES zstd HINTS
            ${_zstd_path}
            PATH_SUFFIXES "lib" "lib64")
    if(ZSTD_LIBRARY)
        message(STATUS "zstd lib: ${ZSTD_LIBRARY}")
    endif()
    find_library (ZSTD_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}${ZSTD_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
            ${_zstd_path}
            PATH_SUFFIXES "lib" "lib64")
    if(ZSTD_LIBRARY)
        set(ZSTD_LIBRARIES "${ZSTD_LIBRARY}")
    elseif(ZSTD_STATIC_LIB)
        set(ZSTD_LIBRARIES "${ZSTD_STATIC_LIB}")
    endif()
    message(STATUS "Zstd libraries: ${ZSTD_LIBRARIES}")
endif()

message(STATUS "zstd libs: ${ZSTD_LIBRARIES}")
TARGET_LINK_LIBRARIES(testcodegen
        libcodegen
        ${GTest_LIBRARIES}
        #${ZSTD_LIBRARIES}
        #${ZLIB_LIBRARIES}
        #${CURSES_LIBRARY}
        runtime
        )

gtest_add_tests(TARGET testcodegen TEST_PREFIX "")