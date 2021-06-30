CMAKE_MINIMUM_REQUIRED(VERSION 3.9 FATAL_ERROR)

# name of the lambda function
set(LAMBDA_NAME tplxlam)

# enable c++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# only compiles on Linux, check here
if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    message(FATAL_ERROR "AWS cpp runtime only builds on Linux, therefore can build lambda executor only on Linux.")
endif()

find_package(aws-lambda-runtime)

#set(CMAKE_EXE_LINKER_FLAGS "-Wl,--copy-dt-needed-entries")
# cf. https://gitlab.kitware.com/cmake/cmake/issues/17297, fix for boost
#set(_Boost_STACKTRACE_BASIC_HEADERS     "boost/stacktrace.hpp")
#set(_Boost_STACKTRACE_BACKTRACE_HEADERS "boost/stacktrace.hpp")
#set(_Boost_STACKTRACE_ADDR2LINE_HEADERS "boost/stacktrace.hpp")
#set(_Boost_STACKTRACE_NOOP_HEADERS      "boost/stacktrace.hpp")
#
#find_package(Boost 1.66.0 COMPONENTS stacktrace_basic stacktrace_backtrace stacktrace_addr2line stacktrace_noop)
#
#find_package(Boost 1.66.0 COMPONENTS iostreams filesystem stacktrace REQUIRED)

# add nlohmann json
include(ExternalProject)
ExternalProject_Get_Property(json source_dir)
set(json_INCLUDE_DIR ${source_dir}/include)
include_directories(${json_INCLUDE_DIR})


# linking
add_executable(${LAMBDA_NAME} "src/main.cc" "src/lambda_main.cc" "src/sighandler.cc")

# enable export symbols in debug for backtrace
set_target_properties(${LAMBDA_NAME} PROPERTIES ENABLE_EXPORTS 1)

target_link_libraries(${LAMBDA_NAME} PRIVATE AWS::aws-lambda-runtime ${AWSSDK_LINK_LIBRARIES}
        libio libutils libcore ${Boost_LIBRARIES} ${LibMagic_LIBRARIES})
target_compile_features(${LAMBDA_NAME} PRIVATE "cxx_std_11")


# @TODO: in order to reduce size of Lambda, strip JITCompiler away from codegen.
# it bloats the size.
# depend on nlohmann json
add_dependencies(${LAMBDA_NAME} json libio libutils libcore)

# this line creates a target that packages your binary and zips it up
if(AMAZON_LINUX)
    # when compiled on Amazon Linux, no need to package libc. reduces size.
    aws_lambda_package_target(${LAMBDA_NAME} NO_LIBC)
else()
    # need to package full libc
    aws_lambda_package_target(${LAMBDA_NAME})
endif()

# Add all the python dependencies to the zip file
set(PYTHON_RESOURCES_LOC ${CMAKE_CURRENT_SOURCE_DIR}/python38_resources)
set(PYTHON_RESOURCES_ZIP ${PYTHON_RESOURCES_LOC}.zip)
message("PYTHON_RESOURCES_ZIP = ${PYTHON_RESOURCES_ZIP}")
message("PYTHON_RESOURCES_LOC = ${PYTHON_RESOURCES_LOC}")

#add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME} POST_BUILD COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/lambda_function.py . && zip -ur ${LAMBDA_NAME}.zip lambda_function.py)
add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME} POST_BUILD COMMAND unzip -u ${PYTHON_RESOURCES_ZIP} -d ${CMAKE_CURRENT_SOURCE_DIR})
add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME} POST_BUILD COMMAND cp -r ${PYTHON_RESOURCES_LOC}/bin . && zip -ur ${LAMBDA_NAME}.zip bin/)
add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME}
        POST_BUILD COMMAND cp -r ${PYTHON_RESOURCES_LOC}/lib .
        && cp -r ${PYTHON_RESOURCES_LOC}/usr_lib/* lib/python3.8/site-packages/
        && cp ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/*runtime* lib/
        && zip -ur ${LAMBDA_NAME}.zip lib/)
add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME} POST_BUILD COMMAND cp -r ${PYTHON_RESOURCES_LOC}/lib64 . && zip -ur ${LAMBDA_NAME}.zip lib64/)

# add runtime .so file to zip
#add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME}
        #POST_BUILD COMMAND mkdir -p lib && cp ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/*runtime* lib/ && zip -ur ${LAMBDA_NAME}.zip lib/)

# copy libgcc
#add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME}
#        POST_BUILD COMMAND mkdir -p lib && cp ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/*runtime* lib/ && zip -ur ${LAMBDA_NAME}.zip lib/)

# if this fails, use  aws --cli-connect-timeout 6000  lambda update-function-code --function-name tplxlam --zip-file fileb://tplxlam.zip
# update function code...
add_custom_command(TARGET aws-lambda-package-${LAMBDA_NAME}
        POST_BUILD COMMAND aws --cli-connect-timeout 6000 lambda update-function-code --function-name ${LAMBDA_NAME}
        --zip-file fileb://${LAMBDA_NAME}.zip)
