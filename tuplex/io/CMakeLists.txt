file(GLOB_RECURSE SOURCES src/*.cc)
file(GLOB_RECURSE INCLUDES include/*.h)

# add AWS S3 connector if configured
if (BUILD_WITH_AWS)
    # Locate the aws sdk for c++ package.
    find_package(AWSSDK REQUIRED COMPONENTS core s3)
    Message(STATUS "AWSSDK_LINK_LIBRARIES ${AWSSDK_LINK_LIBRARIES}")
endif ()


# find libmagic (brew install libmagic on MacOS)
find_package(LibMagic REQUIRED)
message(STATUS "Found LibMagic ${LibMagic_INCLUDE_DIR}, ${LibMagic_LIBRARIES}")

include_directories("include")
include_directories(${Boost_INCLUDE_DIR})

# Install and build ORC C++ APIs
# Orc provides builds for the following libraries:
# - liblz4, libsnappy, libz, and libzstd
find_package(Protobuf REQUIRED)
get_filename_component(Protobuf_HOME "${Protobuf_INCLUDE_DIRS}" DIRECTORY)

include(ExternalProject)
set(EXTERNAL_INSTALL_LOCATION ${CMAKE_BINARY_DIR}/third_party)
ExternalProject_Add(orc
        GIT_REPOSITORY https://github.com/apache/orc.git
        GIT_TAG main
        TIMEOUT 5
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_CXX_FLAGS="-Wno-poison-system-directories" -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION} -DBUILD_JAVA=OFF -DBUILD_TOOLS=OFF -DBUILD_CPP_TESTS=OFF -DBUILD_POSITION_INDEPENDENT_LIB=ON -DPROTOBUF_HOME=${Protobuf_HOME}
        PREFIX "${EXTERNAL_INSTALL_LOCATION}"
        UPDATE_COMMAND "" # Disable update step: clones the project only once
        BUILD_BYPRODUCTS
            ${EXTERNAL_INSTALL_LOCATION}/lib/liborc.a
            ${EXTERNAL_INSTALL_LOCATION}/lib/liblz4.a
            ${EXTERNAL_INSTALL_LOCATION}/lib/libsnappy.a
            ${EXTERNAL_INSTALL_LOCATION}/lib/libz.a
            ${EXTERNAL_INSTALL_LOCATION}/lib/libzstd.a
        )
set(orc_INCLUDE_DIR ${EXTERNAL_INSTALL_LOCATION}/include)
ExternalProject_Get_Property(orc binary_dir)
set(orc_LIBRARY ${EXTERNAL_INSTALL_LOCATION}/lib/liborc.a)

add_library(liborc STATIC IMPORTED)
set_target_properties(liborc PROPERTIES IMPORTED_LOCATION ${orc_LIBRARY})

add_dependencies(liborc orc)
include_directories(${orc_INCLUDE_DIR})

add_library(libio STATIC
        ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES} ${INCLUDES})
set_target_properties(libio PROPERTIES PREFIX "")

target_include_directories(libio PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR} ${Boost_INCLUDE_DIR}
        ${LibMagic_INCLUDE_DIR}
        ${orc_INCLUDE_DIR})

#Note: If awssdk not found, then awssdk_link_librarires is empty...
# Specify here the libraries this program depends on
target_link_libraries(libio libutils
        ${AWSSDK_LINK_LIBRARIES} ${LibMagic_LIBRARIES}
        ${EXTERNAL_INSTALL_LOCATION}/lib/liblz4.a
        ${EXTERNAL_INSTALL_LOCATION}/lib/libsnappy.a
        ${EXTERNAL_INSTALL_LOCATION}/lib/libz.a
        ${EXTERNAL_INSTALL_LOCATION}/lib/libzstd.a
        liborc)

install(TARGETS libio DESTINATION bin)