# Locate pcre2: modified from https://github.com/GenericMappingTools/gmt/blob/master/cmake/modules/FindPCRE2.cmake
#
# This module accepts the following environment variables:
#
#    PCRE2_DIR - Specify the location of PCRE2
#
# This module defines the following CMake variables:
#
#    PCRE2_FOUND - True if libpcre2 is found
#    PCRE2_LIBRARY - A variable pointing to the PCRE2 library
#    PCRE2_INCLUDE_DIR - Where to find the headers

#=============================================================================
# Inspired by FindGDAL
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file Copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See COPYING-CMAKE-SCRIPTS for more information.
#=============================================================================

# This makes the presumption that you are include pcre2.h like
#
#include "pcre2.h"

if (DEFINED PCRE2_ROOT AND NOT PCRE2_ROOT)
    set (PCRE2_LIBRARY "" CACHE INTERNAL "")
    set (PCRE2_INCLUDE_DIR "" CACHE INTERNAL "")
    return ()
endif (DEFINED PCRE2_ROOT AND NOT PCRE2_ROOT)

if (UNIX AND NOT PCRE2_FOUND)
    # Use pcre2-config to obtain the library location and name, something like
    # -L/sw/lib -lpcre2-8)
    find_program (PCRE2_CONFIG pcre2-config
            HINTS
            ${PCRE2_DIR}
            $ENV{PCRE2_DIR}
            PATH_SUFFIXES bin
            PATHS
            ~/Library/Frameworks/pcre2/include/
            /Library/Frameworks/pcre2/include/
            /usr/local/include/
            /usr/local/include/pcre2
            /usr/local
            /usr
            /usr/include/
            /opt
            /opt/local
            ${PCRE2_DIR}/include/
            ${PCRE2_DIR}
            )

    if (PCRE2_CONFIG)
        execute_process (COMMAND ${PCRE2_CONFIG} --cflags
                ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE
                OUTPUT_VARIABLE PCRE2_CONFIG_CFLAGS)
        if (PCRE2_CONFIG_CFLAGS)
            string (REGEX MATCHALL "-I[^ ]+" _pcre2_dashI ${PCRE2_CONFIG_CFLAGS})
            string (REGEX REPLACE "-I" "" _pcre2_includepath "${_pcre2_dashI}")
            string (REGEX REPLACE "-I[^ ]+" "" _pcre2_cflags_other ${PCRE2_CONFIG_CFLAGS})
        endif (PCRE2_CONFIG_CFLAGS)
        execute_process (COMMAND ${PCRE2_CONFIG} --libs8
                ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE
                OUTPUT_VARIABLE PCRE2_CONFIG_LIBS)
        if (PCRE2_CONFIG_LIBS)
            string (REGEX MATCHALL "-l[^ ]+" _pcre2_dashl ${PCRE2_CONFIG_LIBS})
            string (REGEX REPLACE "-l" "" _pcre2_lib "${_pcre2_dashl}")
            string (REGEX MATCHALL "-L[^ ]+" _pcre2_dashL ${PCRE2_CONFIG_LIBS})
            string (REGEX REPLACE "-L" "" _pcre2_libpath "${_pcre2_dashL}")
        endif (PCRE2_CONFIG_LIBS)
    endif (PCRE2_CONFIG)
endif (UNIX AND NOT PCRE2_FOUND)

## find the pcre2 include directory
find_path(PCRE2_INCLUDE_DIR pcre2.h
        HINTS
        ${_pcre2_includepath}
        ${PCRE2_DIR}
        $ENV{PCRE2_DIR}
        PATH_SUFFIXES include include/pcre2 include/PCRE2
        PATHS
        $ENV{CPLUS_INCLUDE_PATH}
        $ENV{C_INCLUDE_PATH}
        ~/Library/Frameworks/pcre2/include/
        /Library/Frameworks/pcre2/include/
        /usr/local/include/
        /usr/local/include/pcre2
        /usr/local
        /usr
        /usr/include/
        /opt
        /opt/local)

# find the pcre2 library
find_library(PCRE2_LIBRARY
        NAMES ${_pcre2_lib} pcre2-8 PCRE2
        HINTS
        ${PCRE2_DIR}
        $ENV{PCRE2_DIR}
        ${_pcre2_libpath}
        PATH_SUFFIXES lib64 lib
        PATHS
        ~/Library/Frameworks
        $ENV{LD_LIBRARY_PATH}
        /Library/Frameworks
        /usr/local
        /usr
        /sw
        /opt/local
        /opt)

# handle the QUIETLY and REQUIRED arguments and set PCRE2_FOUND to TRUE if all listed variables are TRUE
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(pcre2 DEFAULT_MSG PCRE2_LIBRARY PCRE2_INCLUDE_DIR)

set (PCRE2_LIBRARIES ${PCRE2_LIBRARY})
set (PCRE2_INCLUDE_DIRS ${PCRE2_INCLUDE_DIR})