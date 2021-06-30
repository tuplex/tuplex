# from https://raw.githubusercontent.com/lbaehren/CMakeModules/master/FindLibMagic.cmake
#-------------------------------------------------------------------------------
# Copyright (c) 2013-2013, Lars Baehren <lbaehren@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#  * Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#-------------------------------------------------------------------------------

# - Check for the presence of LibMagic
#
# The following variables are set when LibMagic is found:
#  LibMagic_FOUND      = Set to true, if all components of LibMagic have been
#                        found.
#  LibMagic_INCLUDE_DIR   = Include path for the header files of LibMagic
#  LibMagic_LIBRARIES  = Link these to use LibMagic
#  LibMagic_LFLAGS     = Linker flags (optional)

if (NOT LibMagic_FOUND)

    if (NOT LibMagic_ROOT_DIR)
        set (LibMagic_ROOT_DIR ${CMAKE_INSTALL_PREFIX})
    endif (NOT LibMagic_ROOT_DIR)

    ##____________________________________________________________________________
    ## Check for the header files

    find_path (LibMagic_FILE_H
            NAMES file/file.h
            HINTS ${LibMagic_ROOT_DIR} ${CMAKE_INSTALL_PREFIX}
            PATH_SUFFIXES include
            )
    if (LibMagic_FILE_H)
        list (APPEND LibMagic_INCLUDE_DIR ${LibMagic_FILE_H})
    endif (LibMagic_FILE_H)

    find_path (LibMagic_MAGIC_H
            NAMES magic.h
            HINTS ${LibMagic_ROOT_DIR} ${CMAKE_INSTALL_PREFIX}
            PATH_SUFFIXES include include/linux
            )
    if (LibMagic_MAGIC_H)
        list (APPEND LibMagic_INCLUDE_DIR ${LibMagic_MAGIC_H})
    endif (LibMagic_MAGIC_H)

    list (REMOVE_DUPLICATES LibMagic_INCLUDE_DIR)

    ##____________________________________________________________________________
    ## Check for the library

    find_library (LibMagic_LIBRARIES magic
            HINTS ${LibMagic_ROOT_DIR} ${CMAKE_INSTALL_PREFIX}
            PATH_SUFFIXES lib
            )

    ##____________________________________________________________________________
    ## Actions taken when all components have been found

    find_package_handle_standard_args (LibMagic DEFAULT_MSG LibMagic_LIBRARIES LibMagic_INCLUDE_DIR)

    if (LibMagic_FOUND)
        if (NOT LibMagic_FIND_QUIETLY)
            message (STATUS "Found components for LibMagic")
            message (STATUS "LibMagic_ROOT_DIR  = ${LibMagic_ROOT_DIR}")
            message (STATUS "LibMagic_INCLUDE_DIR  = ${LibMagic_INCLUDE_DIR}")
            message (STATUS "LibMagic_LIBRARIES = ${LibMagic_LIBRARIES}")
        endif (NOT LibMagic_FIND_QUIETLY)
    else (LibMagic_FOUND)
        if (LibMagic_FIND_REQUIRED)
            message (FATAL_ERROR "Could not find LibMagic!")
        endif (LibMagic_FIND_REQUIRED)
    endif (LibMagic_FOUND)

    ##____________________________________________________________________________
    ## Mark advanced variables

    mark_as_advanced (
            LibMagic_ROOT_DIR
            LibMagic_INCLUDE_DIR
            LibMagic_LIBRARIES
    )

endif (NOT LibMagic_FOUND)