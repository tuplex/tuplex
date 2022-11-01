find_package(Threads REQUIRED)

## some tests require the cloudpickle package to be installed, hence check for it here
## try first to find full dev version of python, if that fails - interpreter only.
#find_package(Python3 COMPONENTS Interpreter Development QUIET)
#if (Python3_FOUND)
#    message(STATUS "Found full python3-dev installation")
#    set(Python3_Embed_FOUND TRUE)
#else()
#    find_package(Python3 COMPONENTS Interpreter REQUIRED)
#    # python3 -c 'import distutils.sysconfig; print(distutils.sysconfig.get_python_lib(plat_specific=False,standard_lib=True))'
#    # try to get get module libs at least
#
#    # mark embed lib as not found
#    unset(Python3_Embed_FOUND)
#endif()

if (Python3_FOUND)
    # check that cloudpickle is installed via import
    set(cmd -c "import cloudpickle")
    execute_process(COMMAND ${Python3_EXECUTABLE} ${cmd} RESULT_VARIABLE ret)
    if(NOT "${ret}" STREQUAL "0")
        message(FATAL_ERROR "Could not find cloudpickle module, please install via pip3 install cloudpickle.")
    endif()

    # check that numpy is installed too for testing purposes...
    set(cmd -c "import numpy")
    execute_process(COMMAND ${Python3_EXECUTABLE} ${cmd} RESULT_VARIABLE ret)
    if(NOT "${ret}" STREQUAL "0")
        message(FATAL_ERROR "Could not find numpy module, please install via pip3 install numpy.")
    endif()

else()
    message(FATAL_ERROR "could not find python3 interpreter")
endif()

# check whether googletest is locally installed, if not download and fetch
find_package(GTest CONFIG)
if(NOT GTest_FOUND)
    
    # new way of including googletest
    # Download and unpack googletest at configure time
    configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/test/googletest-download )
    if(result)
        message(FATAL_ERROR "CMake step for googletest failed: ${result}")
    endif()
    execute_process(COMMAND ${CMAKE_COMMAND} --build .
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/test/googletest-download )
    if(result)
        message(FATAL_ERROR "Build step for googletest failed: ${result}")
    endif()

    # Prevent overriding the parent project's compiler/linker
    # settings on Windows
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

    # Add googletest directly to our build. This defines
    # the gtest and gtest_main targets.
    # this here picks up potentially a different python version, but not used in build.
    add_subdirectory(${CMAKE_BINARY_DIR}/googletest-src
            ${CMAKE_BINARY_DIR}/googletest-build
            EXCLUDE_FROM_ALL)
    message(STATUS "gtest done (may display different python version before)")
    set(GTest_LIBRARIES "gtest")
else()
    message(STATUS "Using locally installed GoogleTest")
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(GTest_LIBRARIES GTest::gtest)
endif()


add_subdirectory(codegen)
add_subdirectory(io)
add_subdirectory(runtime)
add_subdirectory(adapters)
add_subdirectory(utils)
add_subdirectory(dict)

# these require python, so only if embed is active!
if(Python3_Embed_FOUND)
    add_subdirectory(core)
    add_subdirectory(wrappers)
else()
    message(WARNING "deactivating C++ tests for core/wrappers because no full Python dev installation found.")
endif()

# Resources:::
# copy resources folder
file(COPY resources DESTINATION ${DIST_DIR})
# copy resources folder one more time (little hack, but this is where ctest needs the files)
# it uses build/test as working directory
file(COPY resources DESTINATION ${CMAKE_BINARY_DIR}/test)
