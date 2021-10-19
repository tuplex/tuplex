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

# For MacOS, check whether certain 3rd party libs are already installed via brew
if(BREW_FOUND)
    if(APPLE)
        # Snappy
        EXECUTE_PROCESS(COMMAND brew list snappy OUTPUT_VARIABLE BREW_SNAPPY_LIST ERROR_VARIABLE BREW_SNAPPY_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
        if(BREW_SNAPPY_NOTFOUND)
            set(SNAPPY_LIBRARIES "${EXTERNAL_INSTALL_LOCATION}/lib/libsnappy.a")
        else()
            EXECUTE_PROCESS(COMMAND brew --prefix snappy OUTPUT_VARIABLE BREW_SNAPPY_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
            set(ENV{SNAPPY_HOME} ${BREW_SNAPPY_DIR})
            set(SNAPPY_HOME ${BREW_SNAPPY_DIR})
            message(STATUS "Found locally installed snappy under $ENV{SNAPPY_HOME}")
            # set variables
            file (TO_CMAKE_PATH "${SNAPPY_HOME}" _snappy_path)
            find_library (SNAPPY_LIBRARY NAMES snappy HINTS
                    ${_snappy_path}
                    PATH_SUFFIXES "lib" "lib64")
            if(SNAPPY_LIBRARY)
                message(STATUS "snappy lib: ${SNAPPY_LIBRARY}")
            endif()
            find_library (SNAPPY_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
                    ${_snappy_path}
                    PATH_SUFFIXES "lib" "lib64")
            if(SNAPPY_LIBRARY)
                set(SNAPPY_LIBRARIES "${SNAPPY_LIBRARY}")
            elseif(SNAPPY_STATIC_LIB)
                set(SNAPPY_LIBRARIES "${SNAPPY_STATIC_LIB}")
            endif()
            message(STATUS "Snappy libraries: ${SNAPPY_LIBRARIES}")
        endif()

        # Lz4
        EXECUTE_PROCESS(COMMAND brew list lz4 OUTPUT_VARIABLE BREW_LZ4_LIST ERROR_VARIABLE BREW_LZ4_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
        if (BREW_LZ4_NOTFOUND)
            set(LZ4_LIBRARIES "${EXTERNAL_INSTALL_LOCATION}/lib/liblz4.a")
        else()
            EXECUTE_PROCESS(COMMAND brew --prefix lz4 OUTPUT_VARIABLE BREW_LZ4_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
            set(ENV{LZ4_HOME} ${BREW_LZ4_DIR})
            set(LZ4_HOME ${BREW_LZ4_DIR})
            message(STATUS "Found locally installed lz4 under $ENV{LZ4_HOME}")
            # set variables
            file (TO_CMAKE_PATH "${LZ4_HOME}" _lz4_path)
            find_library (LZ4_LIBRARY NAMES lz4 HINTS
                    ${_lz4_path}
                    PATH_SUFFIXES "lib" "lib64")
            if(LZ4_LIBRARY)
                message(STATUS "lz4 lib: ${LZ4_LIBRARY}")
            endif()
            find_library (LZ4_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}${LZ4_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
                    ${_lz4_path}
                    PATH_SUFFIXES "lib" "lib64")
            if(LZ4_LIBRARY)
                set(LZ4_LIBRARIES "${LZ4_LIBRARY}")
            elseif(LZ4_STATIC_LIB)
                set(LZ4_LIBRARIES "${LZ4_STATIC_LIB}")
            endif()
            message(STATUS "Lz4 libraries: ${LZ4_LIBRARIES}")
        endif()

        # Zstd
        EXECUTE_PROCESS(COMMAND brew list zstd OUTPUT_VARIABLE BREW_ZSTD_LIST ERROR_VARIABLE BREW_ZSTD_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
        if (BREW_ZSTD_NOTFOUND)
            set(ZSTD_LIBRARIES "${EXTERNAL_INSTALL_LOCATION}/lib/libzstd.a")
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

        # Zlib
        EXECUTE_PROCESS(COMMAND brew list zlib OUTPUT_VARIABLE BREW_ZLIB_LIST ERROR_VARIABLE BREW_ZLIB_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
        if (BREW_ZLIB_NOTFOUND)
            set(ZLIB_LIBRARIES "${EXTERNAL_INSTALL_LOCATION}/lib/libz.a")
        else()
            EXECUTE_PROCESS(COMMAND brew --prefix zlib OUTPUT_VARIABLE BREW_ZLIB_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
            set(ENV{ZLIB_HOME} ${BREW_ZLIB_DIR})
            set(ZLIB_HOME ${BREW_ZLIB_DIR})
            message(STATUS "Found locally installed zlib under $ENV{ZLIB_HOME}")
            # set variables
            file (TO_CMAKE_PATH "${ZLIB_HOME}" _zlib_path)
            find_library (ZLIB_LIBRARY NAMES z HINTS
                    ${_zlib_path}
                    PATH_SUFFIXES "lib" "lib64")
            if(ZLIB_LIBRARY)
                message(STATUS "zlib lib: ${ZLIB_LIBRARY}")
            endif()
            find_library (ZLIB_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}${ZLIB_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
                    ${_zlib_path}
                    PATH_SUFFIXES "lib" "lib64")
            if(ZLIB_LIBRARY)
                set(ZLIB_LIBRARIES "${ZLIB_LIBRARY}")
            elseif(ZLIB_STATIC_LIB)
                set(ZLIB_LIBRARIES "${ZLIB_STATIC_LIB}")
            endif()
            message(STATUS "Zlib libraries: ${ZLIB_LIBRARIES}")
        endif()
    endif()
endif()

# set to third-party build
if(NOT SNAPPY_LIBRARIES)
    set(SNAPPY_HOME "")
    set(SNAPPY_LIBRARIES ${EXTERNAL_INSTALL_LOCATION}/lib/libsnappy.a)
endif()

if(NOT LZ4_LIBRARIES)
    set(LZ4_HOME "")
    set(LZ4_LIBRARIES ${EXTERNAL_INSTALL_LOCATION}/lib/liblz4.a)
endif()

if(NOT ZSTD_LIBRARIES)
    set(ZSTD_HOME "")
    set(ZSTD_LIBRARIES ${EXTERNAL_INSTALL_LOCATION}/lib/libzstd.a)
endif()

if(NOT ZLIB_LIBRARIES)
    set(ZLIB_HOME "")
    set(ZLIB_LIBRARIES ${EXTERNAL_INSTALL_LOCATION}/lib/libz.a)
endif()


ExternalProject_Add(orc
        GIT_REPOSITORY https://github.com/apache/orc.git
        GIT_TAG rel/release-1.7.0
        TIMEOUT 5
        CMAKE_ARGS -DBUILD_LIBHDFSPP=OFF -DPROTOBUF_HOME=${Protobuf_HOME} -DSNAPPY_HOME=${SNAPPY_HOME} -DLZ4_HOME=${LZ4_HOME} -DZSTD_HOME=${ZSTD_HOME} -DZLIB_HOME=${ZLIB_HOME} -DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_CXX_FLAGS="-Wno-poison-system-directories" -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION} -DBUILD_JAVA=OFF -DBUILD_TOOLS=OFF -DBUILD_CPP_TESTS=OFF -DBUILD_POSITION_INDEPENDENT_LIB=ON
        PREFIX "${EXTERNAL_INSTALL_LOCATION}"
        UPDATE_COMMAND "" # Disable update step: clones the project only once
        BUILD_BYPRODUCTS
        ${EXTERNAL_INSTALL_LOCATION}/lib/liborc.a
        ${LZ4_LIBRARIES}
        ${SNAPPY_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${ZSTD_LIBRARIES}
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
        ${SNAPPY_LIBRARIES}
        ${LZ4_LIBRARIES}
        ${ZSTD_LIBRARIES}
        ${ZLIB_LIBRARIES}
        liborc)

install(TARGETS libio DESTINATION bin)