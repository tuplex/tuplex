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

add_library(libio STATIC
        ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES} ${INCLUDES})
set_target_properties(libio PROPERTIES PREFIX "")

target_include_directories(libio PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR} ${Boost_INCLUDE_DIR}
        ${LibMagic_INCLUDE_DIR})

#Note: If awssdk not found, then awssdk_link_librarires is empty...
# Specify here the libraries this program depends on
target_link_libraries(libio libutils
        ${AWSSDK_LINK_LIBRARIES} ${LibMagic_LIBRARIES})

install(TARGETS libio DESTINATION bin)