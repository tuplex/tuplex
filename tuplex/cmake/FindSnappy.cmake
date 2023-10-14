# From https://github.com/BVLC/caffe/blob/master/cmake/Modules/FindSnappy.cmake
# Find the Snappy libraries
#
# The following variables are optionally searched for defaults
#  Snappy_ROOT_DIR:    Base directory where all Snappy components are found
#
# The following are set after configuration is done:
#  SNAPPY_FOUND
#  Snappy_INCLUDE_DIR
#  Snappy_LIBRARIES

################################################################################################
# Reads set of version defines from the header file
# Usage:
#   caffe_parse_header(<file> <define1> <define2> <define3> ..)
macro(caffe_parse_header FILENAME FILE_VAR)
  set(vars_regex "")
  set(__parnet_scope OFF)
  set(__add_cache OFF)
  foreach(name ${ARGN})
    if("${name}" STREQUAL "PARENT_SCOPE")
      set(__parnet_scope ON)
    elseif("${name}" STREQUAL "CACHE")
      set(__add_cache ON)
    elseif(vars_regex)
      set(vars_regex "${vars_regex}|${name}")
    else()
      set(vars_regex "${name}")
    endif()
  endforeach()
  if(EXISTS "${FILENAME}")
    file(STRINGS "${FILENAME}" ${FILE_VAR} REGEX "#define[ \t]+(${vars_regex})[ \t]+[0-9]+" )
  else()
    unset(${FILE_VAR})
  endif()
  foreach(name ${ARGN})
    if(NOT "${name}" STREQUAL "PARENT_SCOPE" AND NOT "${name}" STREQUAL "CACHE")
      if(${FILE_VAR})
        if(${FILE_VAR} MATCHES ".+[ \t]${name}[ \t]+([0-9]+).*")
          string(REGEX REPLACE ".+[ \t]${name}[ \t]+([0-9]+).*" "\\1" ${name} "${${FILE_VAR}}")
        else()
          set(${name} "")
        endif()
        if(__add_cache)
          set(${name} ${${name}} CACHE INTERNAL "${name} parsed from ${FILENAME}" FORCE)
        elseif(__parnet_scope)
          set(${name} "${${name}}" PARENT_SCOPE)
        endif()
      else()
        unset(${name} CACHE)
      endif()
    endif()
  endforeach()
endmacro()


find_path(Snappy_INCLUDE_DIR NAMES snappy.h
                             PATHS ${SNAPPY_ROOT_DIR} ${SNAPPY_ROOT_DIR}/include)

find_library(Snappy_LIBRARIES NAMES snappy
                              PATHS ${SNAPPY_ROOT_DIR} ${SNAPPY_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Snappy DEFAULT_MSG Snappy_INCLUDE_DIR Snappy_LIBRARIES)

if(SNAPPY_FOUND)
  message(STATUS "Found Snappy  (include: ${Snappy_INCLUDE_DIR}, library: ${Snappy_LIBRARIES})")
  mark_as_advanced(Snappy_INCLUDE_DIR Snappy_LIBRARIES)

  caffe_parse_header(${Snappy_INCLUDE_DIR}/snappy-stubs-public.h
                     SNAPPY_VERION_LINES SNAPPY_MAJOR SNAPPY_MINOR SNAPPY_PATCHLEVEL)
  set(Snappy_VERSION "${SNAPPY_MAJOR}.${SNAPPY_MINOR}.${SNAPPY_PATCHLEVEL}")
endif()
