# (c) 2018 L.Spiegelberg
# contains util classes + definitions
file(GLOB_RECURSE SOURCES src/*.cc src/*.c)
file(GLOB_RECURSE INCLUDES include/*.h)

include_directories("include")

### include fmtlib/fmt
include(ExternalProject)
set(EXTERNAL_INSTALL_LOCATION ${CMAKE_BINARY_DIR}/third_party)
ExternalProject_Add(fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG 6.2.0
        GIT_CONFIG advice.detachedHead=false
        TIMEOUT 5
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        PREFIX "${EXTERNAL_INSTALL_LOCATION}"
        CONFIGURE_COMMAND "" # Disable configure step
        BUILD_COMMAND "" # Disable build step
        INSTALL_COMMAND "" # Disable install step
        UPDATE_COMMAND "" # Disable update step: clones the project only once
        )
ExternalProject_Get_Property(fmt source_dir)
set(fmt_INCLUDE_DIR ${source_dir}/include)

# add https://github.com/gabime/spdlog
set(EXTERNAL_INSTALL_LOCATION ${CMAKE_BINARY_DIR}/third_party)
ExternalProject_Add(spdlog
        GIT_REPOSITORY https://github.com/gabime/spdlog.git
        GIT_TAG v1.3.1
        GIT_CONFIG advice.detachedHead=false
        TIMEOUT 5
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} #-DSPDLOG_BUILD_BENCH=OFF
        PREFIX "${EXTERNAL_INSTALL_LOCATION}"
        CONFIGURE_COMMAND "" # Disable configure step
        BUILD_COMMAND "" # Disable build step
        INSTALL_COMMAND "" # Disable install step
        UPDATE_COMMAND "" # Disable update step: clones the project only once
        )

# Specify include dir
ExternalProject_Get_Property(spdlog source_dir)
set(spdlog_INCLUDE_DIR ${source_dir}/include)

# Add cJSON ==> newer AWS SDKs ship it, so exclude it then...
# AWS SDK defined cjson since v1.5
if(NOT BUILD_WITH_AWS)
    include(FetchContent)
    set(FETCHCONTENT_BASE_DIR ${CMAKE_BINARY_DIR}/third_party/src)
    FetchContent_Declare(cJSON
        GIT_REPOSITORY https://github.com/DaveGamble/cJSON.git
        GIT_TAG        v1.7.14
        GIT_CONFIG advice.detachedHead=false
    )
    FetchContent_GetProperties(cJSON)
    if(NOT cjson_Populated)
        FetchContent_Populate(cJSON)
        message(STATUS "cJSON Source Dir: ${cjson_SOURCE_DIR}")
        message(STATUS "cJSON Binary Dir: ${cjson_BINARY_DIR}")
    #    add_subdirectory(${cjson_SOURCE_DIR} ${cjson_BINARY_DIR})
    endif()
else()
    message(STATUS "Using cJSON shipped with AWSSDK")
    # Note: alternative is to use CJSON_HIDE_SYMBOLS IWTH AWS SDK.
endif()

# AWS SDK defines cjson since v1.5, yet if using BUILD_WITH_AWS=OFF make sure to add cJSON symbols...
if(NOT BUILD_WITH_AWS)
    list(APPEND SOURCES ${cjson_SOURCE_DIR}/cJSON.c)
endif()

add_library(libutils OBJECT
        ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES} ${INCLUDES})
set_target_properties(libutils PROPERTIES PREFIX "")

### include nlohmann/json
ExternalProject_Add(json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG v3.11.2
        GIT_CONFIG advice.detachedHead=false
        TIMEOUT 5
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        PREFIX "${EXTERNAL_INSTALL_LOCATION}"
        CONFIGURE_COMMAND "" # Disable configure step
        BUILD_COMMAND "" # Disable build step
        INSTALL_COMMAND "" # Disable install step
        UPDATE_COMMAND "" # Disable update step: clones the project only once
        )
ExternalProject_Get_Property(json source_dir)
set(json_INCLUDE_DIR ${source_dir}/include)
include_directories(${json_INCLUDE_DIR})

# ------
# dependencies

add_dependencies(libutils fmt spdlog json)

target_include_directories(libutils PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR}
        ${fmt_INCLUDE_DIR}
        ${spdlog_INCLUDE_DIR}
        ${cjson_SOURCE_DIR}
        ${json_INCLUDE_DIR}
        ${Boost_INCLUDE_DIR}
        ${AWSSDK_INCLUDE_DIR})

# Specify here the libraries this program depends on
target_link_libraries(libutils Boost::filesystem Boost::thread Boost::system Boost::system Boost::iostreams ${AWSSDK_LINK_LIBRARIES})
