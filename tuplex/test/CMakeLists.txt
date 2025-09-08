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
    message(STATUS "Python3 executable: ${Python3_EXECUTABLE}")
    message(STATUS "Python3 version: ${Python3_VERSION}")
    
    # First, let's see what packages are actually installed
    message(STATUS "Checking installed Python packages...")
    execute_process(
        COMMAND ${Python3_EXECUTABLE} -c "import pkg_resources; installed = [d.project_name for d in pkg_resources.working_set]; print('\\n'.join(sorted(installed)))"
        OUTPUT_VARIABLE installed_packages
        RESULT_VARIABLE ret_packages
    )
    if("${ret_packages}" STREQUAL "0")
        message(STATUS "Installed packages:")
        message(STATUS "${installed_packages}")
    else()
        message(WARNING "Could not list installed packages")
    endif()
    
    # Check Python path and site-packages
    execute_process(
        COMMAND ${Python3_EXECUTABLE} -c "import sys; print('\\n'.join(sys.path))"
        OUTPUT_VARIABLE python_path
        RESULT_VARIABLE ret_path
    )
    if("${ret_path}" STREQUAL "0")
        message(STATUS "Python sys.path:")
        message(STATUS "${python_path}")
    endif()
    
    # check that cloudpickle is installed via import
    message(STATUS "Testing cloudpickle import...")
    set(cmd -c "import cloudpickle; print('cloudpickle version:', cloudpickle.__version__)")
    execute_process(
        COMMAND ${Python3_EXECUTABLE} ${cmd} 
        OUTPUT_VARIABLE cloudpickle_output
        ERROR_VARIABLE cloudpickle_error
        RESULT_VARIABLE ret
    )
    if(NOT "${ret}" STREQUAL "0")
        message(STATUS "cloudpickle import failed with return code: ${ret}")
        message(STATUS "cloudpickle stdout: ${cloudpickle_output}")
        message(STATUS "cloudpickle stderr: ${cloudpickle_error}")
        message(FATAL_ERROR "Could not find cloudpickle module. Please install via 'pip3 install cloudpickle'. The module is required for testing. Check the output above for details about what packages are currently installed.")
    else()
        message(STATUS "cloudpickle import successful: ${cloudpickle_output}")
    endif()

    # check that numpy is installed too for testing purposes...
    message(STATUS "Testing numpy import...")
    set(cmd -c "import numpy; print('numpy version:', numpy.__version__)")
    execute_process(
        COMMAND ${Python3_EXECUTABLE} ${cmd}
        OUTPUT_VARIABLE numpy_output
        ERROR_VARIABLE numpy_error
        RESULT_VARIABLE ret
    )
    if(NOT "${ret}" STREQUAL "0")
        message(STATUS "numpy import failed with return code: ${ret}")
        message(STATUS "numpy stdout: ${numpy_output}")
        message(STATUS "numpy stderr: ${numpy_error}")
        message(FATAL_ERROR "Could not find numpy module. Please install via 'pip3 install numpy'. The module is required for testing. Check the output above for details about what packages are currently installed.")
    else()
        message(STATUS "numpy import successful: ${numpy_output}")
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
    set(GTest_LIBRARIES GTest::gtest GTest::gtest_main)
endif()

if(BUILD_WITH_AWS)
    #set(Protobuf_USE_STATIC_LIBS ON)
    # https://github.com/protocolbuffers/protobuf/issues/12637
    find_package(Protobuf CONFIG)
    if(Protobuf_NOTFOUND)
        find_package(Protobuf REQUIRED)
    endif()
endif()

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

# newer gtest >= 1.13 needs abseil
if(GTest_VERSION VERSION_GREATER_EQUAL 1.13)
    # find abseil & amend libs
    find_package(absl REQUIRED)
endif()

# add subdirs after above config is done
add_subdirectory(codegen)
add_subdirectory(io)
add_subdirectory(runtime)
add_subdirectory(adapters)
add_subdirectory(utils)