# Locate Celero benchmarking library
#
# This module defines
#  CELERO_FOUND, if false, do not try to link to yaml-cpp
#  CELERO_LIBRARY, where to find celero
#  CELERO_INCLUDE_DIR, where to find Celero.h
#
# If Celero is not installed in a standard path, you can use the CELERO_DIR CMake variable
# to tell CMake where celero is.

## find the celero include directory
find_path(CELERO_INCLUDE_DIR celero/Celero.h
        PATH_SUFFIXES include
        PATHS
        $ENV{CPLUS_INCLUDE_PATH}
        $ENV{C_INCLUDE_PATH}
        ~/Library/Frameworks/celero/include/
        /Library/Frameworks/celero/include/
        /usr/local/include/
        /usr/local/include/celero
        /usr/local
        /usr
        /usr/include/
        /opt
        /opt/local
        ${CELERO_DIR}/include/
        ${CELERO_DIR})

# find the yaml-cpp library
find_library(CELERO_LIBRARY
        NAMES celero
        PATH_SUFFIXES lib64 lib
        PATHS ~/Library/Frameworks
        $ENV{LD_LIBRARY_PATH}
        /Library/Frameworks
        /usr/local
        /usr
        /sw
        /opt/local
        /opt
        ${CELERO_DIR}/lib
        ${CELERO_DIR})

# handle the QUIETLY and REQUIRED arguments and set YAMLCPP_FOUND to TRUE if all listed variables are TRUE
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CELERO DEFAULT_MSG CELERO_INCLUDE_DIR CELERO_LIBRARY)
mark_as_advanced(CELERO_INCLUDE_DIR CELERO_LIBRARY)