cmake_minimum_required(VERSION 3.10)

project(weldq19)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT DEFINED ENV{WELD_HOME})
    message(FATAL_ERROR "WELD_HOME not set")
endif()

find_package(ZLIB REQUIRED)

if(UNIX AND NOT APPLE)
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
    find_package(Curses REQUIRED)
endif()

if(UNIX AND APPLE)
    # global config, brew there?
    FIND_PROGRAM(BREW_FOUND "brew")
    set(CURSES_NEED_NCURSES "TRUE")
    set(CURSES_NEED_WIDE "TRUE")
    EXECUTE_PROCESS(COMMAND brew --prefix ncurses OUTPUT_VARIABLE NCURSES_ROOT_DIR ERROR_VARIABLE BREW_NCURSES_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(BREW_NCURSES_NOTFOUND)
        message("Could not find ncurses, install via brew install ncurses")
    endif()
    set(CURSES_LIBRARIES "${NCURSES_ROOT_DIR}/lib/libncurses.a")
    set(CMAKE_EXE_LINKER_FLAGS "-framework Security ")
endif()

# activate SSE4.2 for faster parsing...
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")
set(WELD_LIBRARY $ENV{WELD_HOME}/target/release/libweld.a)
message(STATUS "WELD library found in ${WELD_LIBRARY}")
add_executable(weldq19 main.cc)
include_directories(weldq19 $ENV{WELD_HOME}/weld-capi)
if(UNIX AND NOT APPLE)
    target_link_libraries(weldq19 ${WELD_LIBRARY} ${CURSES_LIBRARIES} ${ZLIB_LIBRARIES} Threads::Threads ${CMAKE_DL_LIBS})
else()
    target_link_libraries(weldq19 ${WELD_LIBRARY} ${CURSES_LIBRARIES} ${ZLIB_LIBRARIES})
endif()
