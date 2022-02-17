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


# Install and build ORC C++ APIs when BUILD_WITH_ORC is active
if(BUILD_WITH_ORC)
    message(STATUS "Building Tuplex with ORC support")

    find_package(Protobuf REQUIRED)
    get_filename_component(Protobuf_HOME "${Protobuf_INCLUDE_DIRS}" DIRECTORY)

    include(ExternalProject)
    set(EXTERNAL_INSTALL_LOCATION ${CMAKE_BINARY_DIR}/third_party)

    # For MacOS, check whether certain 3rd party libs are already installed via brew
    if(BREW_FOUND)
        if(APPLE)
            set(THIRDPARTY_CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}")

            # Snappy
            EXECUTE_PROCESS(COMMAND brew list snappy OUTPUT_VARIABLE BREW_SNAPPY_LIST ERROR_VARIABLE BREW_SNAPPY_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
            if(BREW_SNAPPY_NOTFOUND)
                message(STATUS "Could not find locally installed snappy, building third party")
                set(SNAPPY_VERSION "1.1.7")
                set(SNAPPY_HOME "${EXTERNAL_INSTALL_LOCATION}")
                set(SNAPPY_INCLUDE_DIR "${SNAPPY_HOME}/include")
                set(SNAPPY_STATIC_LIB "${SNAPPY_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}snappy${CMAKE_STATIC_LIBRARY_SUFFIX}")
                set(SNAPPY_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${SNAPPY_HOME}
                        -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_LIBDIR=lib -DSNAPPY_BUILD_TESTS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
                ExternalProject_Add (snappy_ep
                        URL "https://github.com/google/snappy/archive/${SNAPPY_VERSION}.tar.gz"
                        CMAKE_ARGS ${SNAPPY_CMAKE_ARGS}
                        BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}")

                set(SNAPPY_LIBRARIES ${SNAPPY_STATIC_LIB})

                add_library(snappy INTERFACE)
                target_link_libraries(snappy INTERFACE ${SNAPPY_STATIC_LIB})
                target_include_directories(snappy SYSTEM INTERFACE ${SNAPPY_INCLUDE_DIR})

                add_dependencies(snappy snappy_ep)
                install(FILES "${SNAPPY_STATIC_LIB}" DESTINATION "lib")
                set(SNAPPY_DEPENDS "snappy_ep")
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
            if(BREW_LZ4_NOTFOUND)
                message(STATUS "Could not find locally installed lz4, building third party")
                set(LZ4_VERSION "1.7.5")
                set(LZ4_HOME "${EXTERNAL_INSTALL_LOCATION}")
                set(LZ4_INCLUDE_DIR "${LZ4_HOME}/include")
                set(LZ4_STATIC_LIB "${LZ4_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}lz4${CMAKE_STATIC_LIBRARY_SUFFIX}")
                set(LZ4_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${LZ4_HOME}
                        -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_LIBDIR=lib -DLZ4_BUILD_TESTS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON)

                if (CMAKE_VERSION VERSION_GREATER "3.7")
                    set(LZ4_CONFIGURE SOURCE_SUBDIR "contrib/cmake_unofficial" CMAKE_ARGS ${LZ4_CMAKE_ARGS})
                else()
                    set(LZ4_CONFIGURE CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}" ${LZ4_CMAKE_ARGS}
                            "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix/src/lz4_ep/contrib/cmake_unofficial")
                endif()

                ExternalProject_Add (lz4_ep
                        URL "https://github.com/lz4/lz4/archive/v${LZ4_VERSION}.tar.gz"
                        ${LZ4_CONFIGURE}
                        BUILD_BYPRODUCTS "${LZ4_STATIC_LIB}")

                set(LZ4_LIBRARIES ${LZ4_STATIC_LIB})

                add_library(lz4 INTERFACE)
                target_link_libraries(lz4 INTERFACE ${LZ4_STATIC_LIB})
                target_include_directories(lz4 SYSTEM INTERFACE ${LZ4_INCLUDE_DIR})

                add_dependencies(lz4 lz4_ep)
                install(FILES "${LZ4_STATIC_LIB}" DESTINATION "lib")
                set(LZ4_DEPENDS "lz4_ep")
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

            # Zlib
            EXECUTE_PROCESS(COMMAND brew list zlib OUTPUT_VARIABLE BREW_ZLIB_LIST ERROR_VARIABLE BREW_ZLIB_NOTFOUND OUTPUT_STRIP_TRAILING_WHITESPACE)
            if(BREW_ZLIB_NOTFOUND)
                message(STATUS "Could not find locally installed zlib, building third party")
                set(ZLIB_VERSION "1.2.11") # cloudflare's
                set(ZLIB_HOME "${EXTERNAL_INSTALL_LOCATION}")
                set(ZLIB_INCLUDE_DIR "${ZLIB_HOME}/include")
                set(ZLIB_STATIC_LIB "${ZLIB_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}z${CMAKE_STATIC_LIBRARY_SUFFIX}")
                set(ZLIB_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ZLIB_HOME}
                        -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_LIBDIR=lib -DZLIB_BUILD_TESTS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
                # link is broken, do not use.
                #  ExternalProject_Add (zlib_ep
                #          URL "http://zlib.net/fossils/zlib-${ZLIB_VERSION}.tar.gz"
                #          CMAKE_ARGS ${ZLIB_CMAKE_ARGS}
                #          BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}")

                # use cloudflare's fork or zlib-ng
                ExternalProject_Add (zlib_ep
                        GIT_REPOSITORY https://github.com/cloudflare/zlib.git
                        GIT_TAG origin/gcc.amd64
                        TIMEOUT 5
                        PREFIX "${EXTERNAL_INSTALL_LOCATION}"
                        UPDATE_COMMAND "" # Disable update step: clones the project only once
                        CMAKE_ARGS ${ZLIB_CMAKE_ARGS}
                        BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}")

                # # Alternative, use zlib-ng
                # ExternalProject_Add (zlib_ep
                #         GIT_REPOSITORY https://github.com/zlib-ng/zlib-ng.git
                #         GIT_TAG 2.0.5
                #         TIMEOUT 5
                #         PREFIX "${EXTERNAL_INSTALL_LOCATION}"
                #         UPDATE_COMMAND "" # Disable update step: clones the project only once
                #         CMAKE_ARGS ${ZLIB_CMAKE_ARGS}
                #         BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}")

                set(ZLIB_LIBRARIES ${ZLIB_STATIC_LIB})

                add_library(zlib INTERFACE)
                target_link_libraries(zlib INTERFACE ${ZLIB_STATIC_LIB})
                target_include_directories(zlib SYSTEM INTERFACE ${ZLIB_INCLUDE_DIR})

                add_dependencies(zlib zlib_ep)
                install(FILES "${ZLIB_STATIC_LIB}" DESTINATION "lib")
                set(ZLIB_DEPENDS "zlib_ep")
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
                find_library (ZLIB_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}z${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
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

    if (NOT APPLE)
        message(STATUS "Adding byproducts to external project")
        set(SNAPPY_LIBRARIES ${EXTERNAL_INSTALL_LOCATION}/lib/libsnappy.a)
        set(ZSTD_LIBRARIES ${EXTERNAL_INSTALL_LOCATION}/lib/libzstd.a)
        set(ZLIB_LIBRARIES ${EXTERNAL_INSTALL_LOCATION}/lib/libz.a)
        set(LZ4_LIBRARIES ${EXTERNAL_INSTALL_LOCATION}/lib/liblz4.a)
        set(ORC_THIRD_PARTY_LIBS
            ${SNAPPY_LIBRARIES}
            ${ZSTD_LIBRARIES}
            ${ZLIB_LIBRARIES}
            ${LZ4_LIBRARIES})
    endif()

    # if supported, set no-poioson-system-directories. Requires Clang > 10.0
    if(CMAKE_CXX_COMPILED_ID STREQUAL "Clang")
      # set if CLANG version > 10
      if(CMAKE_CXX_COMPILER_VERSION_GREATER_EQUAL 10.0)
          ucm_add_flags("-Wno-poison-system-directories")
      endif()
    elseif(CMAKE_CXX_COMPILED_ID STREQUAL "GNU")
        ucm_add_flags("-Wno-poison-system-directories")
    endif()
    message(STATUS "Configuring ORC to run with flags: ${CMAKE_CXX_FLAGS}")
    ExternalProject_Add(orc
            GIT_REPOSITORY https://github.com/apache/orc.git
            GIT_TAG rel/release-1.7.3
            TIMEOUT 5
            CMAKE_ARGS -DBUILD_LIBHDFSPP=OFF -DSNAPPY_HOME=${SNAPPY_HOME} -DLZ4_HOME=${LZ4_HOME} -DZSTD_HOME=${ZSTD_HOME} -DZLIB_HOME=${ZLIB_HOME} -DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION} -DSTOP_BUILD_ON_WARNING=OFF -DBUILD_JAVA=OFF -DBUILD_TOOLS=OFF -DBUILD_CPP_TESTS=OFF -DBUILD_POSITION_INDEPENDENT_LIB=ON -DPROTOBUF_HOME=${Protobuf_HOME}
            PREFIX "${EXTERNAL_INSTALL_LOCATION}"
            UPDATE_COMMAND "" # Disable update step: clones the project only once
            BUILD_BYPRODUCTS ${EXTERNAL_INSTALL_LOCATION}/lib/liborc.a ${ORC_THIRD_PARTY_LIBS}
            )
    ExternalProject_Add_StepDependencies(orc build ${SNAPPY_DEPENDS} ${LZ4_DEPENDS} ${ZSTD_DEPENDS} ${ZLIB_DEPENDS})
    set(orc_INCLUDE_DIR ${EXTERNAL_INSTALL_LOCATION}/include)
    ExternalProject_Get_Property(orc binary_dir)
    set(orc_LIBRARY ${EXTERNAL_INSTALL_LOCATION}/lib/liborc.a)

    add_library(liborc STATIC IMPORTED)
    set_target_properties(liborc PROPERTIES IMPORTED_LOCATION ${orc_LIBRARY})

    add_dependencies(liborc orc)
    include_directories(${orc_INCLUDE_DIR})

    set(ORC_LIBRARIES
            ${SNAPPY_LIBRARIES}
            ${ZSTD_LIBRARIES}
            ${ZLIB_LIBRARIES}
            ${LZ4_LIBRARIES}
            liborc)
endif()

add_library(libio OBJECT
        ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES} ${INCLUDES})
set_target_properties(libio PROPERTIES PREFIX "")

target_include_directories(libio PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR} ${Boost_INCLUDE_DIR}
        ${LibMagic_INCLUDE_DIR}
        ${orc_INCLUDE_DIR})

#Note: If awssdk not found, then awssdk_link_librarires is empty...
# Specify here the libraries this program depends on
target_link_libraries(libio libutils
        ${AWSSDK_LINK_LIBRARIES}
        ${LibMagic_LIBRARIES}
        ${ORC_LIBRARIES})

install(TARGETS libio DESTINATION bin)
