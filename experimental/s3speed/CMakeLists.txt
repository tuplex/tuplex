
CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)
project(s3speed)

# enable c++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# enable c11
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(SRCS main.cc md5.cc)

find_package(AWSSDK REQUIRED COMPONENTS core s3 s3-crt)
Message(STATUS "AWSSDK_LINK_LIBRARIES ${AWSSDK_LINK_LIBRARIES}")

# linking
add_executable(s3speed ${SRCS})

# enable export symbols in debug for backtrace
set_target_properties(s3speed PROPERTIES ENABLE_EXPORTS 1)

target_link_libraries(s3speed PRIVATE ${AWSSDK_LINK_LIBRARIES})
target_compile_features(s3speed PRIVATE "cxx_std_11")
