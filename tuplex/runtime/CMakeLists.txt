# Runtime written in C to support UDF execution, as separate module to allow easier updates
# (c) 2018 L.Spiegelberg

file(GLOB_RECURSE SOURCES src/*.cc)
file(GLOB_RECURSE INCLUDES include/*.h)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(runtime SHARED
        ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES} ${INCLUDES})

# change from cmake default libruntime to tuplex_runtime
set_target_properties(runtime PROPERTIES PREFIX "")
set_target_properties(runtime PROPERTIES OUTPUT_NAME "tuplex_runtime")
set(RUNTIME_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
message(STATUS "Tuplex python language runtime include dir is: ${RUNTIME_INCLUDE_DIR}")

target_include_directories(runtime PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include ${PCRE2_INCLUDE_DIRS})

# keep dependencies lean...
target_link_libraries(runtime libutils ${PCRE2_LIBRARIES})

# require thread_local and aligned malloc keyword (C11 or C++11)
target_compile_features(runtime PRIVATE cxx_thread_local)

# copy dylib to pip package
add_custom_command(TARGET runtime POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E copy
        "$<TARGET_FILE:runtime>"
        "${DIST_DIR}/python/tuplex/libexec/$<TARGET_FILE_NAME:runtime>"
        COMMENT "Copying runtime shared library to pip package directory")

# copy also to bin (for easier running)
add_custom_command(TARGET runtime POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E copy
        "$<TARGET_FILE:runtime>"
        "${DIST_DIR}/bin/$<TARGET_FILE_NAME:runtime>"
        COMMENT "Copying runtime shared library to bin directory")