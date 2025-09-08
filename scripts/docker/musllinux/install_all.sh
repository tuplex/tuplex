#!/bin/sh
# (c) Tuplex team 2017-2023
# Master script that installs all tuplex dependencies in the correct order.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python3}

# Start script.
set -euxo pipefail

# need to run this with root privileges
if [ "$(id -u)" -ne 0 ]; then
  echo "Please run this script with root privileges"
  exit 1
fi

echo ">> Starting installation of all Tuplex dependencies"

# Create base directories
echo ">> Creating base directories"
mkdir -p $PREFIX && chmod 0755 $PREFIX
mkdir -p $PREFIX/sbin
mkdir -p $PREFIX/bin
mkdir -p $PREFIX/share
mkdir -p $PREFIX/include
mkdir -p $PREFIX/lib

echo ">> Files will be downloaded to ${WORKDIR}/tuplex-downloads"
DOWNLOAD_DIR=$WORKDIR/tuplex-downloads
mkdir -p $DOWNLOAD_DIR

# Set environment variables
export CC=${CC:-gcc}
export CXX=${CXX:-g++}
export PATH=$PREFIX/bin:$PATH

# 1. Install Alpine packages and CMake (base system dependencies)
echo ">> Step 1: Installing Alpine packages and CMake"
./install_alpine.sh

# 2. Install zlib-ng (required by AWS SDK)
echo ">> Step 2: Installing zlib-ng"
./install_zlib.sh

# 3. Install LLVM (compiler infrastructure)
echo ">> Step 3: Installing LLVM"
./install_llvm.sh

# 4. Install Boost (C++ libraries)
echo ">> Step 4: Installing Boost"
./install_boost.sh

# 5. Install Google Test (testing framework)
echo ">> Step 5: Installing Google Test"
./install_googletest.sh

# 6. Install Snappy (compression library)
echo ">> Step 6: Installing Snappy"
./install_snappy.sh

# 7. Install YAML-CPP (YAML parsing)
echo ">> Step 7: Installing YAML-CPP"
./install_yamlcpp.sh

# 8. Install Celero (benchmarking)
echo ">> Step 8: Installing Celero"
./install_celero.sh

# 9. Install ANTLR4 (parser generator)
echo ">> Step 9: Installing ANTLR4"
./install_antlr.sh

# 10. Install PCRE2 (regex library)
echo ">> Step 10: Installing PCRE2"
./install_pcre2.sh

# 11. Install Protocol Buffers (serialization)
echo ">> Step 11: Installing Protocol Buffers"
./install_protobuf.sh

# 12. Install AWS SDK C++ and Lambda runtime
echo ">> Step 12: Installing AWS SDK C++ and Lambda runtime"
./install_aws_sdk.sh

# Clean up downloads
echo ">> Cleaning up download directory"
rm -rf ${DOWNLOAD_DIR}

echo ">> All Tuplex dependencies installed successfully to ${PREFIX}"

# Verify that key tools are available
echo ">> Verifying installation..."
ls -la /opt/bin/
echo ">> PATH: $PATH"
echo ">> CMake version:"
/opt/bin/cmake --version || echo "CMake not found in /opt/bin"
echo ">> Python version:"
python3 --version || echo "Python3 not found"

echo "-- Done, all Tuplex requirements installed to /opt --"
