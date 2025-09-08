#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs ANTLR4 dependencies required to build tuplex on Alpine Linux.
#
# KNOWN ISSUE: The ANTLR C++ runtime build may fail due to CMake version
# compatibility issues with its embedded Google Test dependency. This script
# will fail explicitly if the build doesn't succeed.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
ANTLR4_VERSION=${ANTLR4_VERSION:-4.13.1}
CC=${CC:-gcc}
CXX=${CXX:-g++}

# Get CPU count for parallel builds
CPU_COUNT=$(( 1 * $( grep '^processor[[:space:]]*:' /proc/cpuinfo | wc -l ) ))

# Start script.
set -euxo pipefail

# Cleanup function
cleanup() {
    if [ -d "${DOWNLOAD_DIR}/antlr" ]; then
        echo ">> Cleaning up on exit..."
        rm -rf "${DOWNLOAD_DIR}/antlr"
    fi
}

# Set trap to cleanup on exit
trap cleanup EXIT

# need to run this with root privileges
if [ "$(id -u)" -ne 0 ]; then
  echo "Please run this script with root privileges"
  exit 1
fi

echo ">> Installing ANTLR4 ${ANTLR4_VERSION}"

# Create download directory
DOWNLOAD_DIR=${WORKDIR}/tuplex-downloads
mkdir -p ${DOWNLOAD_DIR}/antlr && cd ${DOWNLOAD_DIR}/antlr

echo ">> Downloading ANTLR JAR file..."
curl -O https://www.antlr.org/download/antlr-${ANTLR4_VERSION}-complete.jar
cp antlr-${ANTLR4_VERSION}-complete.jar ${PREFIX}/lib/
echo ">> ANTLR JAR file installed successfully"

echo ">> Downloading ANTLR C++ runtime source..."
curl -O https://www.antlr.org/download/antlr4-cpp-runtime-${ANTLR4_VERSION}-source.zip
unzip antlr4-cpp-runtime-${ANTLR4_VERSION}-source.zip -d antlr4-cpp-runtime
rm antlr4-cpp-runtime-${ANTLR4_VERSION}-source.zip

echo ">> Building ANTLR C++ runtime..."
cd antlr4-cpp-runtime
echo ">> Removing existing build directory..."
rm -rf build
echo ">> Creating fresh build directory..."
mkdir -p build
echo ">> Entering build directory..."
cd build
echo ">> Current working directory: $(pwd)"

echo ">> Running cmake configuration..."
echo ">> Note: ANTLR C++ runtime has embedded Google Test with CMake compatibility issues"
echo ">> Attempting to configure with policy overrides..."
echo ">> Current directory: $(pwd)"
echo ">> Running cmake command..."
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} -DCMAKE_POLICY_VERSION_MINIMUM=3.5 ..
echo ">> Cmake exit code: $?"
if [ $? -ne 0 ]; then
    echo ">> ERROR: cmake configuration failed"
    echo ">> This is likely due to CMake version compatibility issues with embedded Google Test"
    echo ">> ANTLR C++ runtime requires CMake 3.15+ but has embedded dependencies requiring older versions"
    echo ">> Consider using a pre-built ANTLR C++ runtime package instead of building from source"
    exit 1
fi
echo ">> Cmake configuration completed successfully"

echo ">> Compiling ANTLR C++ runtime..."
echo ">> Current directory: $(pwd)"
echo ">> Checking for Makefile..."
ls -la Makefile* || echo ">> No Makefile found!"
echo ">> Running make command..."
make -j ${CPU_COUNT}
echo ">> Make exit code: $?"
if [ $? -ne 0 ]; then
    echo ">> ERROR: make compilation failed"
    echo ">> This indicates that the cmake configuration did not generate proper build files"
    echo ">> The ANTLR C++ runtime build is not working properly"
    exit 1
fi
echo ">> Make compilation completed successfully"

echo ">> Installing ANTLR C++ runtime..."
make install
if [ $? -ne 0 ]; then
    echo ">> ERROR: make install failed"
    exit 1
fi

echo ">> Verifying ANTLR C++ runtime installation..."
if ! ls -la ${PREFIX}/lib/libantlr4-runtime* >/dev/null 2>&1; then
    echo ">> ERROR: ANTLR C++ runtime library not found after installation"
    echo ">> Expected files in ${PREFIX}/lib/:"
    ls -la ${PREFIX}/lib/ | grep -i antlr || echo ">> No antlr files found in lib directory"
    exit 1
fi

if ! ls -la ${PREFIX}/include/antlr4-runtime/ >/dev/null 2>&1; then
    echo ">> ERROR: ANTLR C++ runtime headers not found after installation"
    echo ">> Expected directory: ${PREFIX}/include/antlr4-runtime/"
    echo ">> Contents of ${PREFIX}/include/:"
    ls -la ${PREFIX}/include/ || echo ">> Include directory not found"
    exit 1
fi

echo ">> ANTLR C++ runtime verification successful"

# Clean up build artifacts
echo ">> Cleaning up build artifacts..."
cd ${DOWNLOAD_DIR}
rm -rf antlr

echo ">> ANTLR4 ${ANTLR4_VERSION} installation completed successfully"
