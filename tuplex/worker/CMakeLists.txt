CMAKE_MINIMUM_REQUIRED(VERSION 3.9 FATAL_ERROR)

# name of the executable
set(WORKER_NAME tuplex-worker)

# enable c++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# enable c11
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# add nlohmann json
include(ExternalProject)
ExternalProject_Get_Property(json source_dir)
set(json_INCLUDE_DIR ${source_dir}/include)
include_directories(${json_INCLUDE_DIR})

include_directories(include)

set(WORKER_SRC src/main.cc)
# linking
add_executable(${WORKER_NAME} WORKER_SRCS)

# enable export symbols in debug for backtrace
set_target_properties(${WORKER_NAME} PROPERTIES ENABLE_EXPORTS 1)

target_link_libraries(${WORKER_NAME} PRIVATE ${AWSSDK_LINK_LIBRARIES}
        libio libutils libcore ${Boost_LIBRARIES} ${LibMagic_LIBRARIES})
target_compile_features(${WORKER_NAME} PRIVATE "cxx_std_11")

# @TODO: in order to reduce size of Lambda, strip JITCompiler away from 0codegen.
# it bloats the size.
# depend on nlohmann json
add_dependencies(${WORKER_NAME} json libio libutils libcore)