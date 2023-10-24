# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (C) 2022, Arne Wendt
#

# vcpkg examples use 3.0.0, assuming this as minimum version for vcpkg cmake toolchain
cmake_minimum_required(VERSION 3.0.0)

# config:
# - VCPKG_VERSION:
#   - "latest": latest git tag (undefined or empty treated as "latest")
#   - "edge": last commit on master
# - VCPKG_PARENT_DIR: where to place vcpkg
# - VCPKG_FORCE_SYSTEM_BINARIES: use system cmake, zip, unzip, tar, etc.
#       may be necessary on some systems as downloaded binaries may be linked against unsupported libraries
#       musl-libc based distros (ALPINE)(!) require use of system binaries, but are AUTO DETECTED!
# - VCPKG_FEATURE_FLAGS: modify feature flags; default are "manifests,versions"
#
# - VCPKG_NO_INIT: do not call vcpkg_init() automatically (for use testing)


# set default feature flags if not defined
if(NOT DEFINED VCPKG_FEATURE_FLAGS)
    set(VCPKG_FEATURE_FLAGS "manifests,versions" CACHE INTERNAL "necessary vcpkg flags for manifest based autoinstall and versioning")
endif()

# disable metrics by default
if(NOT DEFINED VCPKG_METRICS_FLAG)
    set(VCPKG_METRICS_FLAG "-disableMetrics" CACHE INTERNAL "flag to disable telemtry by default")
endif()

# enable rebuilding of packages if requested by changed configuration
if(NOT DEFINED VCPKG_RECURSE_REBUILD_FLAG)
    set(VCPKG_RECURSE_REBUILD_FLAG "--recurse" CACHE INTERNAL "enable rebuilding of packages if requested by changed configuration by default")
endif()


# check_conditions and find neccessary packages
find_package(Git REQUIRED)



# get VCPKG
function(vcpkg_init)
    # set environment (not cached)

    # mask musl-libc if masked prior
    if(VCPKG_MASK_MUSL_LIBC)
        vcpkg_mask_if_musl_libc()
    endif()

    # use system binaries
    if(VCPKG_FORCE_SYSTEM_BINARIES)
        set(ENV{VCPKG_FORCE_SYSTEM_BINARIES} "1")
    endif()

    # for use in scripting mode
    if(CMAKE_SCRIPT_MODE_FILE)
        if(VCPKG_TARGET_TRIPLET)
            set(ENV{VCPKG_DEFAULT_TRIPLET} "${VCPKG_DEFAULT_TRIPLET}")
        endif()
        if(VCPKG_DEFAULT_TRIPLET)
            set(ENV{VCPKG_DEFAULT_TRIPLET} "${VCPKG_DEFAULT_TRIPLET}")
        endif()
        if(VCPKG_HOST_TRIPLET)
            set(ENV{VCPKG_DEFAULT_HOST_TRIPLET} "${VCPKG_DEFAULT_HOST_TRIPLET}")
        endif()
        if(VCPKG_DEFAULT_HOST_TRIPLET)
            set(ENV{VCPKG_DEFAULT_HOST_TRIPLET} "${VCPKG_DEFAULT_HOST_TRIPLET}")
        endif()
    endif()
    # end set environment


    # test for vcpkg availability
    # executable path set ? assume all ok : configure
    if(VCPKG_EXECUTABLE EQUAL "" OR NOT DEFINED VCPKG_EXECUTABLE)
        # configure vcpkg

        # use system binaries?
        # IMPORTANT: we have to use system binaries on musl-libc systems, as vcpkg fetches binaries linked against glibc!
        vcpkg_set_use_system_binaries_flag()

        # mask musl-libc if no triplet is provided
        if(
        ( ENV{VCPKG_DEFAULT_TRIPLET} EQUAL "" OR NOT DEFINED ENV{VCPKG_DEFAULT_TRIPLET}) AND
        ( ENV{VCPKG_DEFAULT_HOST_TRIPLET} EQUAL "" OR NOT DEFINED ENV{VCPKG_DEFAULT_HOST_TRIPLET}) AND
        ( VCPKG_TARGET_TRIPLET EQUAL "" OR NOT DEFINED VCPKG_TARGET_TRIPLET)
        )
            # mask musl-libc from vcpkg
            vcpkg_mask_if_musl_libc()
        else()
            message(WARNING "One of VCPKG_TARGET_TRIPLET, ENV{VCPKG_DEFAULT_TRIPLET} or ENV{VCPKG_DEFAULT_HOST_TRIPLET} has been defined. NOT CHECKING FOR musl-libc MASKING!")
        endif()


        # test options
        if(VCPKG_PARENT_DIR EQUAL "" OR NOT DEFINED VCPKG_PARENT_DIR)
            if(CMAKE_SCRIPT_MODE_FILE)
                message(FATAL_ERROR "Explicitly specify VCPKG_PARENT_DIR when running in script mode!")
            else()
                message(STATUS "VCPKG from: ${CMAKE_CURRENT_BINARY_DIR}")
                set(VCPKG_PARENT_DIR "${CMAKE_CURRENT_BINARY_DIR}/")
            endif()
        endif()
        string(REGEX REPLACE "[/\\]$" "" VCPKG_PARENT_DIR "${VCPKG_PARENT_DIR}")

        # test if VCPKG_PARENT_DIR has to be created in script mode
        if(CMAKE_SCRIPT_MODE_FILE AND NOT EXISTS "${VCPKG_PARENT_DIR}")
            message(STATUS "Creating vcpkg parent directory")
            file(MAKE_DIRECTORY "${VCPKG_PARENT_DIR}")
        endif()


        # set path/location varibles to expected path; necessary to detect after a CMake cache clean
        vcpkg_set_vcpkg_directory_from_parent()
        vcpkg_set_vcpkg_executable()

        # executable is present ? configuring done : fetch and build
        execute_process(COMMAND ${VCPKG_EXECUTABLE} version RESULT_VARIABLE VCPKG_TEST_RETVAL OUTPUT_VARIABLE VCPKG_VERSION_BANNER)
        if(NOT VCPKG_TEST_RETVAL EQUAL "0")
            # reset executable path to prevent malfunction/wrong assumptions in case of error
            set(VCPKG_EXECUTABLE "")

            # getting vcpkg
            message(STATUS "No VCPKG executable found; getting new version ready...")

            # select compile script
            if(WIN32)
                set(VCPKG_BUILD_CMD ".\\bootstrap-vcpkg.bat")
            else()
                set(VCPKG_BUILD_CMD "./bootstrap-vcpkg.sh")
            endif()

            # prepare and clone git sources
            # include(FetchContent)
            # set(FETCHCONTENT_QUIET on)
            # set(FETCHCONTENT_BASE_DIR "${VCPKG_PARENT_DIR}")
            # FetchContent_Declare(
            #     vcpkg

            #     GIT_REPOSITORY "https://github.com/microsoft/vcpkg"
            #     GIT_PROGRESS true

            #     SOURCE_DIR "${VCPKG_PARENT_DIR}/vcpkg"
            #     BINARY_DIR ""
            #     BUILD_IN_SOURCE true
            #     CONFIGURE_COMMAND ""
            #     BUILD_COMMAND ""
            # )
            # FetchContent_Populate(vcpkg)

            # check for bootstrap script ? ok : fetch repository
            if(NOT EXISTS "${VCPKG_DIRECTORY}/${VCPKG_BUILD_CMD}" AND NOT EXISTS "${VCPKG_DIRECTORY}\\${VCPKG_BUILD_CMD}")
                message(STATUS "VCPKG bootstrap script not found; fetching...")
                # directory existent ? delete
                if(EXISTS "${VCPKG_DIRECTORY}")
                    file(REMOVE_RECURSE "${VCPKG_DIRECTORY}")
                endif()

                # fetch vcpkg repo
                execute_process(COMMAND ${GIT_EXECUTABLE} clone https://github.com/microsoft/vcpkg WORKING_DIRECTORY "${VCPKG_PARENT_DIR}" RESULT_VARIABLE VCPKG_GIT_CLONE_OK)
                if(NOT VCPKG_GIT_CLONE_OK EQUAL "0")
                    message(FATAL_ERROR "Cloning VCPKG repository from https://github.com/microsoft/vcpkg failed!")
                endif()
            endif()

            # compute git checkout target
            vcpkg_set_version_checkout()

            # hide detached head notice
            execute_process(COMMAND ${GIT_EXECUTABLE} config advice.detachedHead false WORKING_DIRECTORY "${VCPKG_DIRECTORY}" RESULT_VARIABLE VCPKG_GIT_HIDE_DETACHED_HEAD_IGNORED)
            # checkout asked version
            execute_process(COMMAND ${GIT_EXECUTABLE} checkout ${VCPKG_VERSION_CHECKOUT} WORKING_DIRECTORY "${VCPKG_DIRECTORY}" RESULT_VARIABLE VCPKG_GIT_TAG_CHECKOUT_OK)
            if(NOT VCPKG_GIT_TAG_CHECKOUT_OK EQUAL "0")
                message(FATAL_ERROR "Checking out VCPKG version/tag ${VCPKG_VERSION} failed!")
            endif()

            # wrap -disableMetrics in extra single quotes for windows
            # if(WIN32 AND NOT VCPKG_METRICS_FLAG EQUAL "" AND DEFINED VCPKG_METRICS_FLAG)
            #     set(VCPKG_METRICS_FLAG "'${VCPKG_METRICS_FLAG}'")
            # endif()

            # build vcpkg
            execute_process(COMMAND ${VCPKG_BUILD_CMD} ${VCPKG_USE_SYSTEM_BINARIES_FLAG} ${VCPKG_METRICS_FLAG} WORKING_DIRECTORY "${VCPKG_DIRECTORY}" RESULT_VARIABLE VCPKG_BUILD_OK)
            if(NOT VCPKG_BUILD_OK EQUAL "0")
                message(FATAL_ERROR "Bootstrapping VCPKG failed!")
            endif()
            message(STATUS "Built VCPKG!")


            # get vcpkg path
            vcpkg_set_vcpkg_executable()

            # test vcpkg binary
            execute_process(COMMAND ${VCPKG_EXECUTABLE} version RESULT_VARIABLE VCPKG_OK OUTPUT_VARIABLE VCPKG_VERSION_BANNER)
            if(NOT VCPKG_OK EQUAL "0")
                message(FATAL_ERROR "VCPKG executable failed test!")
            endif()

            message(STATUS "VCPKG OK!")
            message(STATUS "Install packages using VCPKG:")
            message(STATUS " * from your CMakeLists.txt by calling vcpkg_add_package(<PKG_NAME>)")
            message(STATUS " * by providing a 'vcpkg.json' in your project directory [https://devblogs.microsoft.com/cppblog/take-control-of-your-vcpkg-dependencies-with-versioning-support/]")

            # generate empty manifest on vcpkg installation if none is found
            if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg.json")
                cmake_language(DEFER DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} CALL vcpkg_manifest_generation_finalize)
                message(STATUS "If you need an empty manifest for setting up your project, you will find one in your bild directory")
            endif()
        endif()

        # we have fetched and built, but a clean has been performed
        # version banner is set while testing for availability or after build
        message(STATUS "VCPKG using:")
        string(REGEX REPLACE "\n.*$" "" VCPKG_VERSION_BANNER "${VCPKG_VERSION_BANNER}")
        message(STATUS "${VCPKG_VERSION_BANNER}")

        # cache executable path
        set(VCPKG_EXECUTABLE ${VCPKG_EXECUTABLE} CACHE STRING "vcpkg executable path" FORCE)

        # initialize manifest generation
        vcpkg_manifest_generation_init()

        # install from manifest if ran in script mode
        if(CMAKE_SCRIPT_MODE_FILE)
            message(STATUS "Running in script mode to setup environment: trying dependency installation from manifest!")
            if(EXISTS "./vcpkg.json")
                message(STATUS "Found vcpkg.json; installing...")
                vcpkg_install_manifest()
            else()
                message(STATUS "NOT found vcpkg.json; skipping installation")
            endif()
        endif()

        # set toolchain
        set(CMAKE_TOOLCHAIN_FILE "${VCPKG_DIRECTORY}/scripts/buildsystems/vcpkg.cmake")
        set(CMAKE_TOOLCHAIN_FILE ${CMAKE_TOOLCHAIN_FILE} PARENT_SCOPE)
        set(CMAKE_TOOLCHAIN_FILE ${CMAKE_TOOLCHAIN_FILE} CACHE STRING "")
    endif()
endfunction()


# # make target triplet from current compiler selection and platform
# # set VCPKG_TARGET_TRIPLET in parent scope
# function(vcpkg_make_set_triplet)
#     # get platform: win/linux ONLY
#     if(WIN32)
#         set(PLATFORM "windows")
#     else()
#         set(PLATFORM "linux")
#     endif()

#     # get bitness: 32/64 ONLY
#     if(CMAKE_SIZEOF_VOID_P EQUAL 8)
#         set(BITS 64)
#     else()
#         set(BITS 86)
#     endif()

#     set(VCPKG_TARGET_TRIPLET "x${BITS}-${PLATFORM}" PARENT_SCOPE)
# endfunction()

# set VCPKG_DIRECTORY to assumed path based on VCPKG_PARENT_DIR
# vcpkg_set_vcpkg_directory_from_parent([VCPKG_PARENT_DIR_EXPLICIT])
function(vcpkg_set_vcpkg_directory_from_parent)
    if(ARGV0 EQUAL "" OR NOT DEFINED ARGV0)
        set(VCPKG_DIRECTORY "${VCPKG_PARENT_DIR}/vcpkg" PARENT_SCOPE)
    else()
        set(VCPKG_DIRECTORY "${ARGV0}/vcpkg" PARENT_SCOPE)
    endif()
    # set(VCPKG_DIRECTORY ${VCPKG_DIRECTORY} CACHE STRING "vcpkg tool location" FORCE)
endfunction()


# set VCPKG_EXECUTABLE to assumed path based on VCPKG_DIRECTORY
# vcpkg_set_vcpkg_executable([VCPKG_DIRECTORY])
function(vcpkg_set_vcpkg_executable)
    if(ARGV0 EQUAL "" OR NOT DEFINED ARGV0)
        set(VCPKG_DIRECTORY_EXPLICIT ${VCPKG_DIRECTORY})
    else()
        set(VCPKG_DIRECTORY_EXPLICIT ${ARGV0})
    endif()

    if(WIN32)
        set(VCPKG_EXECUTABLE "${VCPKG_DIRECTORY_EXPLICIT}/vcpkg.exe" PARENT_SCOPE)
    else()
        set(VCPKG_EXECUTABLE "${VCPKG_DIRECTORY_EXPLICIT}/vcpkg" PARENT_SCOPE)
    endif()
endfunction()

# determine git checkout target in: VCPKG_VERSION_CHECKOUT
# vcpkg_set_version_checkout([VCPKG_VERSION_EXPLICIT] [VCPKG_DIRECTORY_EXPLICIT])
function(vcpkg_set_version_checkout)
    if(ARGV0 EQUAL "" OR NOT DEFINED ARGV0)
        set(VCPKG_VERSION_EXPLICIT ${VCPKG_VERSION})
    else()
        set(VCPKG_VERSION_EXPLICIT ${ARGV0})
    endif()
    if(ARGV1 EQUAL "" OR NOT DEFINED ARGV1)
        set(VCPKG_DIRECTORY_EXPLICIT ${VCPKG_DIRECTORY})
    else()
        set(VCPKG_DIRECTORY_EXPLICIT ${ARGV1})
    endif()

    # get latest git tag
    execute_process(COMMAND git for-each-ref refs/tags/ --count=1 --sort=-creatordate --format=%\(refname:short\) WORKING_DIRECTORY "${VCPKG_DIRECTORY_EXPLICIT}" OUTPUT_VARIABLE VCPKG_GIT_TAG_LATEST)
    string(REGEX REPLACE "\n$" "" VCPKG_GIT_TAG_LATEST "${VCPKG_GIT_TAG_LATEST}")

    # resolve versions
    if(EXISTS "./vcpkg.json")
        # set hash from vcpkg.json manifest
        file(READ "./vcpkg.json" VCPKG_MANIFEST_CONTENTS)

        if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.19)
            string(JSON VCPKG_BASELINE GET "${VCPKG_MANIFEST_CONTENTS}" "builtin-baseline")
        else()
            string(REGEX REPLACE "[\n ]" "" VCPKG_MANIFEST_CONTENTS "${VCPKG_MANIFEST_CONTENTS}")
            string(REGEX MATCH "\"builtin-baseline\":\"[0-9a-f]+\"" VCPKG_BASELINE "${VCPKG_MANIFEST_CONTENTS}")
            string(REPLACE "\"builtin-baseline\":" "" VCPKG_BASELINE "${VCPKG_BASELINE}")
            string(REPLACE "\"" "" VCPKG_BASELINE "${VCPKG_BASELINE}")
        endif()

        if(NOT "${VCPKG_BASELINE}" EQUAL "")
            if(NOT "${VCPKG_VERSION}" EQUAL "" AND DEFINED VCPKG_VERSION)
                message(WARNING "VCPKG_VERSION was specified, but vcpkg.json manifest is used and specifies a builtin-baseline; using builtin-baseline: ${VCPKG_BASELINE}")
            endif()
            set(VCPKG_VERSION_EXPLICIT "${VCPKG_BASELINE}")
            message(STATUS "Using VCPKG Version: <manifest builtin-baseline>")
        endif()
    endif()

    if("${VCPKG_VERSION_EXPLICIT}" STREQUAL "latest" OR "${VCPKG_VERSION_EXPLICIT}" EQUAL "" OR NOT DEFINED VCPKG_VERSION_EXPLICIT)
        set(VCPKG_VERSION_CHECKOUT ${VCPKG_GIT_TAG_LATEST})
        message(STATUS "Using VCPKG Version: ${VCPKG_VERSION_EXPLICIT} (latest)")
    elseif("${VCPKG_VERSION_EXPLICIT}" STREQUAL "edge" OR "${VCPKG_VERSION_EXPLICIT}" STREQUAL "master")
        set(VCPKG_VERSION_CHECKOUT "master")
        message(STATUS "Using VCPKG Version: edge (latest commit)")
    else()
        message(STATUS "Using VCPKG Version: ${VCPKG_VERSION_EXPLICIT}")
        set(VCPKG_VERSION_CHECKOUT ${VCPKG_VERSION_EXPLICIT})
    endif()

    set(VCPKG_VERSION_CHECKOUT ${VCPKG_VERSION_CHECKOUT} PARENT_SCOPE)
endfunction()

# sets VCPKG_PLATFORM_MUSL_LIBC(ON|OFF)
function(vcpkg_get_set_musl_libc)
    if(WIN32 OR APPLE)
        # is windows
        set(VCPKG_PLATFORM_MUSL_LIBC OFF)
    else()
        execute_process(COMMAND getconf GNU_LIBC_VERSION RESULT_VARIABLE VCPKG_PLATFORM_GLIBC)
        if(VCPKG_PLATFORM_GLIBC EQUAL "0")
            # has glibc
            set(VCPKG_PLATFORM_MUSL_LIBC OFF)
        else()
            execute_process(COMMAND ldd --version RESULT_VARIABLE VCPKG_PLATFORM_LDD_OK OUTPUT_VARIABLE VCPKG_PLATFORM_LDD_VERSION_STDOUT ERROR_VARIABLE VCPKG_PLATFORM_LDD_VERSION_STDERR)
            string(TOLOWER "${VCPKG_PLATFORM_LDD_VERSION_STDOUT}" VCPKG_PLATFORM_LDD_VERSION_STDOUT)
            string(TOLOWER "${VCPKG_PLATFORM_LDD_VERSION_STDERR}" VCPKG_PLATFORM_LDD_VERSION_STDERR)
            string(FIND "${VCPKG_PLATFORM_LDD_VERSION_STDOUT}" "musl" VCPKG_PLATFORM_LDD_FIND_MUSL_STDOUT)
            string(FIND "${VCPKG_PLATFORM_LDD_VERSION_STDERR}" "musl" VCPKG_PLATFORM_LDD_FIND_MUSL_STDERR)
            if(
            (VCPKG_PLATFORM_LDD_OK EQUAL "0" AND NOT VCPKG_PLATFORM_LDD_FIND_MUSL_STDOUT EQUAL "-1") OR
            (NOT VCPKG_PLATFORM_LDD_OK EQUAL "0" AND NOT VCPKG_PLATFORM_LDD_FIND_MUSL_STDERR EQUAL "-1")
            )
                # has musl-libc
                # use system binaries
                set(VCPKG_PLATFORM_MUSL_LIBC ON)
                message(STATUS "VCPKG: System is using musl-libc; using system binaries! (e.g. cmake, curl, zip, tar, etc.)")
            else()
                # has error...
                message(FATAL_ERROR "VCPKG: could detect neither glibc nor musl-libc!")
            endif()
        endif()
    endif()

    # propagate back
    set(VCPKG_PLATFORM_MUSL_LIBC ${VCPKG_PLATFORM_MUSL_LIBC} PARENT_SCOPE)
endfunction()


# configure environment and CMake variables to mask musl-libc from vcpkg triplet checks
function(vcpkg_mask_musl_libc)
    # set target triplet without '-musl'
    execute_process(COMMAND ldd --version RESULT_VARIABLE VCPKG_PLATFORM_LDD_OK OUTPUT_VARIABLE VCPKG_PLATFORM_LDD_VERSION_STDOUT ERROR_VARIABLE VCPKG_PLATFORM_LDD_VERSION_STDERR)
    string(TOLOWER "${VCPKG_PLATFORM_LDD_VERSION_STDOUT}" VCPKG_PLATFORM_LDD_VERSION_STDOUT)
    string(TOLOWER "${VCPKG_PLATFORM_LDD_VERSION_STDERR}" VCPKG_PLATFORM_LDD_VERSION_STDERR)
    string(FIND "${VCPKG_PLATFORM_LDD_VERSION_STDOUT}" "x86_64" VCPKG_PLATFORM_LDD_FIND_MUSL_BITS_STDOUT)
    string(FIND "${VCPKG_PLATFORM_LDD_VERSION_STDERR}" "x86_64" VCPKG_PLATFORM_LDD_FIND_MUSL_BITS_STDERR)
    if(
            NOT VCPKG_PLATFORM_LDD_FIND_MUSL_BITS_STDOUT EQUAL "-1" OR
            NOT VCPKG_PLATFORM_LDD_FIND_MUSL_BITS_STDERR EQUAL "-1"
    )
        set(VCPKG_TARGET_TRIPLET "x64-linux")
    else()
        set(VCPKG_TARGET_TRIPLET "x86-linux")
    endif()

    set(ENV{VCPKG_DEFAULT_TRIPLET} "${VCPKG_TARGET_TRIPLET}")
    set(ENV{VCPKG_DEFAULT_HOST_TRIPLET} "${VCPKG_TARGET_TRIPLET}")
    set(VCPKG_TARGET_TRIPLET "${VCPKG_TARGET_TRIPLET}" CACHE STRING "vcpkg default target triplet (possibly dont change)")
    message(STATUS "VCPKG: System is using musl-libc; fixing default target triplet as: ${VCPKG_TARGET_TRIPLET}")

    set(VCPKG_MASK_MUSL_LIBC ON CACHE INTERNAL "masked musl-libc")
endfunction()

# automate musl-libc masking
function(vcpkg_mask_if_musl_libc)
    vcpkg_get_set_musl_libc()
    if(VCPKG_PLATFORM_MUSL_LIBC)
        vcpkg_mask_musl_libc()
    endif()
endfunction()

# sets VCPKG_USE_SYSTEM_BINARIES_FLAG from VCPKG_PLATFORM_MUSL_LIBC and/or VCPKG_FORCE_SYSTEM_BINARIES
# vcpkg_set_use_system_binaries_flag([VCPKG_FORCE_SYSTEM_BINARIES_EXPLICIT])
function(vcpkg_set_use_system_binaries_flag)
    if(ARGV0 EQUAL "" OR NOT DEFINED ARGV0)
        set(VCPKG_FORCE_SYSTEM_BINARIES_EXPLICIT ${VCPKG_FORCE_SYSTEM_BINARIES})
    else()
        set(VCPKG_FORCE_SYSTEM_BINARIES_EXPLICIT ${ARGV0})
    endif()

    vcpkg_get_set_musl_libc()

    if(NOT WIN32 AND (VCPKG_FORCE_SYSTEM_BINARIES_EXPLICIT OR VCPKG_PLATFORM_MUSL_LIBC) )
        set(VCPKG_USE_SYSTEM_BINARIES_FLAG "--useSystemBinaries" PARENT_SCOPE)
        # has to be propagated to all install calls
        set(ENV{VCPKG_FORCE_SYSTEM_BINARIES} "1")
        set(VCPKG_FORCE_SYSTEM_BINARIES ON CACHE BOOL "force vcpkg to use system binaries (possibly dont change)")

        message(STATUS "VCPKG: Requested use of system binaries! (e.g. cmake, curl, zip, tar, etc.)")
    else()
        set(VCPKG_USE_SYSTEM_BINARIES_FLAG "" PARENT_SCOPE)
    endif()
endfunction()


# install package
function(vcpkg_add_package PKG_NAME)
    # if(VCPKG_TARGET_TRIPLET STREQUAL "" OR NOT DEFINED VCPKG_TARGET_TRIPLET)
    #     vcpkg_make_set_triplet()
    # endif()
    set(VCPKG_TARGET_TRIPLET_FLAG "")
    if(DEFINED VCPKG_TARGET_TRIPLET AND NOT VCPKG_TARGET_TRIPLET EQUAL "")
        set(VCPKG_TARGET_TRIPLET_FLAG "--triplet=${VCPKG_TARGET_TRIPLET}")
    endif()

    message(STATUS "VCPKG: fetching ${PKG_NAME} via vcpkg_add_package")
    execute_process(COMMAND ${VCPKG_EXECUTABLE} ${VCPKG_TARGET_TRIPLET_FLAG} ${VCPKG_RECURSE_REBUILD_FLAG} --feature-flags=-manifests --disable-metrics install "${PKG_NAME}" WORKING_DIRECTORY ${CMAKE_SOURCE_DIR} RESULT_VARIABLE VCPKG_INSTALL_OK)
    if(NOT VCPKG_INSTALL_OK EQUAL "0")
        message(FATAL_ERROR "VCPKG: failed fetching ${PKG_NAME}! Did you call vcpkg_init(<...>)?")
    else()
        # add package to automatically generated manifest
        vcpkg_manifest_generation_add_dependency("${PKG_NAME}")
    endif()
endfunction()


# install packages from manifest in script mode
function(vcpkg_install_manifest)
    # if(VCPKG_TARGET_TRIPLET STREQUAL "" OR NOT DEFINED VCPKG_TARGET_TRIPLET)
    #     vcpkg_make_set_triplet()
    # endif()
    # message(STATUS "VCPKG: install from manifest; using target triplet: ${VCPKG_TARGET_TRIPLET}")
    # execute_process(COMMAND ${VCPKG_EXECUTABLE} --triplet=${VCPKG_TARGET_TRIPLET} --feature-flags=manifests,versions --disable-metrics install WORKING_DIRECTORY ${CMAKE_SOURCE_DIR} RESULT_VARIABLE VCPKG_INSTALL_OK)
    get_filename_component(VCPKG_EXECUTABLE_ABS ${VCPKG_EXECUTABLE} ABSOLUTE)
    file(COPY "./vcpkg.json" DESTINATION "${VCPKG_PARENT_DIR}")
    execute_process(COMMAND ${VCPKG_EXECUTABLE_ABS} --feature-flags=manifests,versions --disable-metrics install WORKING_DIRECTORY "${VCPKG_PARENT_DIR}" RESULT_VARIABLE VCPKG_INSTALL_OK)
    if(NOT VCPKG_INSTALL_OK EQUAL "0")
        message(FATAL_ERROR "VCPKG: install from manifest failed")
    endif()
endfunction()

## manifest generation requires CMake > 3.19
function(vcpkg_manifest_generation_update_cache VCPKG_GENERATED_MANIFEST)
    string(REGEX REPLACE "\n" "" VCPKG_GENERATED_MANIFEST "${VCPKG_GENERATED_MANIFEST}")
    set(VCPKG_GENERATED_MANIFEST "${VCPKG_GENERATED_MANIFEST}" CACHE STRING "template for automatically generated manifest by vcpkg-cmake-integration" FORCE)
    mark_as_advanced(FORCE VCPKG_GENERATED_MANIFEST)
endfunction()


# build empty json manifest and register deferred call to finalize and write
function(vcpkg_manifest_generation_init)
    if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.19)
        # init "empty" json and cache variable
        set(VCPKG_GENERATED_MANIFEST "{}")

        # initialize dependencies as empty list
        # first vcpkg_add_package will transform to object and install finalization handler
        # transform to list in finalization step
        string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" dependencies "[]")
        string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" "$schema" "\"https://raw.githubusercontent.com/microsoft/vcpkg/master/scripts/vcpkg.schema.json\"")
        string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" version "\"0.1.0-autogenerated\"")

        # write baseline commit
        execute_process(COMMAND git log --pretty=format:'%H' -1 WORKING_DIRECTORY "${VCPKG_DIRECTORY}" OUTPUT_VARIABLE VCPKG_GENERATED_MANIFEST_BASELINE)
        string(REPLACE "'" "" VCPKG_GENERATED_MANIFEST_BASELINE "${VCPKG_GENERATED_MANIFEST_BASELINE}")
        string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" builtin-baseline "\"${VCPKG_GENERATED_MANIFEST_BASELINE}\"")

        vcpkg_manifest_generation_update_cache("${VCPKG_GENERATED_MANIFEST}")

        # will be initialized from vcpkg_add_package call
        # # defer call to finalize manifest
        # # needs to be called later as project variables are not set when initializing
        # cmake_language(DEFER CALL vcpkg_manifest_generation_finalize)
    endif()
endfunction()

# add dependency to generated manifest
function(vcpkg_manifest_generation_add_dependency PKG_NAME)
    if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.19)
        # extract features
        string(REGEX MATCH "\\[.*\\]" PKG_FEATURES "${PKG_NAME}")
        string(REPLACE "${PKG_FEATURES}" "" PKG_BASE_NAME "${PKG_NAME}")
        # make comma separated list
        string(REPLACE "[" "" PKG_FEATURES "${PKG_FEATURES}")
        string(REPLACE "]" "" PKG_FEATURES "${PKG_FEATURES}")
        string(REPLACE " " "" PKG_FEATURES "${PKG_FEATURES}")
        # build cmake list by separating with ;
        string(REPLACE "," ";" PKG_FEATURES "${PKG_FEATURES}")

        if(NOT PKG_FEATURES)
            # set package name string only
            set(PKG_DEPENDENCY_JSON "\"${PKG_BASE_NAME}\"")
        else()
            # build dependency object with features
            set(PKG_DEPENDENCY_JSON "{}")
            string(JSON PKG_DEPENDENCY_JSON SET "${PKG_DEPENDENCY_JSON}" name "\"${PKG_BASE_NAME}\"")

            set(FEATURE_LIST_JSON "[]")
            foreach(FEATURE IN LISTS PKG_FEATURES)
                if(FEATURE STREQUAL "core")
                    # set default feature option if special feature "core" is specified
                    string(JSON PKG_DEPENDENCY_JSON SET "${PKG_DEPENDENCY_JSON}" default-features "false")
                else()
                    # add feature to list
                    string(JSON FEATURE_LIST_JSON_LEN LENGTH "${FEATURE_LIST_JSON}")
                    string(JSON FEATURE_LIST_JSON SET "${FEATURE_LIST_JSON}" ${FEATURE_LIST_JSON_LEN} "\"${FEATURE}\"")
                endif()
            endforeach()

            # build dependency object with feature list
            string(JSON PKG_DEPENDENCY_JSON SET "${PKG_DEPENDENCY_JSON}" features "${FEATURE_LIST_JSON}")
        endif()

        # add dependency to manifest
        # reset to empty object to avoid collissions and track new packages
        # defer (new) finalization call
        string(JSON VCPKG_GENERATED_MANIFEST_DEPENDENCIES_TYPE TYPE "${VCPKG_GENERATED_MANIFEST}" dependencies)
        if(VCPKG_GENERATED_MANIFEST_DEPENDENCIES_TYPE STREQUAL "ARRAY")
            string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" dependencies "{}")
            cmake_language(DEFER CALL vcpkg_manifest_generation_finalize)
        endif()
        string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" dependencies "${PKG_BASE_NAME}" "${PKG_DEPENDENCY_JSON}")

        vcpkg_manifest_generation_update_cache("${VCPKG_GENERATED_MANIFEST}")
    endif()
endfunction()


# build empty json manifest and register deferred call to finalize and write
function(vcpkg_manifest_generation_finalize)
    if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.19)
        # populate project information
        string(REGEX REPLACE "[^a-z0-9\\.-]" "" VCPKG_GENERATED_MANIFEST_NAME "${PROJECT_NAME}")
        string(TOLOWER VCPKG_GENERATED_MANIFEST_NAME "${VCPKG_GENERATED_MANIFEST_NAME}")
        string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" name "\"${VCPKG_GENERATED_MANIFEST_NAME}\"")
        if(NOT PROJECT_VERSION EQUAL "" AND DEFINED PROJECT_VERSION)
            string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" version "\"${PROJECT_VERSION}\"")
        endif()

        vcpkg_manifest_generation_update_cache("${VCPKG_GENERATED_MANIFEST}")

        # make list from dependency dictionary
        # cache dependency object
        string(JSON VCPKG_GENERATED_DEPENDENCY_OBJECT GET "${VCPKG_GENERATED_MANIFEST}" dependencies)
        # initialize dependencies as list
        string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" dependencies "[]")

        string(JSON VCPKG_GENERATED_DEPENDENCY_COUNT LENGTH "${VCPKG_GENERATED_DEPENDENCY_OBJECT}")
        if(VCPKG_GENERATED_DEPENDENCY_COUNT GREATER 0)
            # setup range stop for iteration
            math(EXPR VCPKG_GENERATED_DEPENDENCY_LOOP_STOP "${VCPKG_GENERATED_DEPENDENCY_COUNT} - 1")

            # make list
            foreach(DEPENDENCY_INDEX RANGE ${VCPKG_GENERATED_DEPENDENCY_LOOP_STOP})
                string(JSON DEPENDENCY_NAME MEMBER "${VCPKG_GENERATED_DEPENDENCY_OBJECT}" ${DEPENDENCY_INDEX})
                string(JSON DEPENDENCY_JSON GET "${VCPKG_GENERATED_DEPENDENCY_OBJECT}" "${DEPENDENCY_NAME}")
                string(JSON DEPENDENCY_JSON_TYPE ERROR_VARIABLE DEPENDENCY_JSON_TYPE_ERROR_IGNORE TYPE "${DEPENDENCY_JSON}")
                if(DEPENDENCY_JSON_TYPE STREQUAL "OBJECT")
                    string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" dependencies ${DEPENDENCY_INDEX} "${DEPENDENCY_JSON}")
                else()
                    string(JSON VCPKG_GENERATED_MANIFEST SET "${VCPKG_GENERATED_MANIFEST}" dependencies ${DEPENDENCY_INDEX} "\"${DEPENDENCY_JSON}\"")
                endif()
            endforeach()
        endif()

        message(STATUS "VCPKG auto-generated manifest (${CMAKE_CURRENT_BINARY_DIR}/vcpkg.json):\n${VCPKG_GENERATED_MANIFEST}")
        file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/vcpkg.json" "${VCPKG_GENERATED_MANIFEST}")
    endif()
endfunction()


# get vcpkg and configure toolchain
if(NOT VCPKG_NO_INIT)
    vcpkg_init()
endif()