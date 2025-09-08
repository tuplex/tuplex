#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs LLVM dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python3}
LLVM_VERSION=${LLVM_VERSION:-16.0.6}
CC=${CC:-gcc}
CXX=${CXX:-g++}

# Start script.
set -euxo pipefail

# need to run this with root privileges
if [ "$(id -u)" -ne 0 ]; then
  echo "Please run this script with root privileges"
  exit 1
fi

echo ">> Installing LLVM ${LLVM_VERSION}"

# Parse LLVM version components
LLVM_MAJOR_VERSION=`echo ${LLVM_VERSION} | cut -d. -f1`
LLVM_MINOR_VERSION=`echo ${LLVM_VERSION} | cut -d. -f2`
LLVM_MAJMIN_VERSION="${LLVM_MAJOR_VERSION}.${LLVM_MINOR_VERSION}"

# list of targets available to build: AArch64;AMDGPU;ARM;AVR;BPF;Hexagon;Lanai;LoongArch;Mips;MSP430;NVPTX;PowerPC;RISCV;Sparc;SystemZ;VE;WebAssembly;X86;XCore
# in order to cross-compile, should use targets:

echo ">> building LLVM ${LLVM_VERSION}"
LLVM_URL=https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_VERSION}/llvm-${LLVM_VERSION}.src.tar.xz
CLANG_URL=https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_VERSION}/clang-${LLVM_VERSION}.src.tar.xz
# required when LLVM version > 15
LLVM_CMAKE_URL=https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_VERSION}/cmake-${LLVM_VERSION}.src.tar.xz

PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python3}
PYTHON_BASENAME="$(basename -- $PYTHON_EXECUTABLE)"
PYTHON_VERSION=$(${PYTHON_EXECUTABLE} --version)
echo ">> Building dependencies for ${PYTHON_VERSION}"

echo ">> Downloading prerequisites for llvm ${LLVM_VERSION}"
LLVM_WORKDIR=${WORKDIR}/tuplex-downloads/llvm${LLVM_VERSION}
mkdir -p ${LLVM_WORKDIR}
cd "${LLVM_WORKDIR}" || exit 1

wget ${LLVM_URL} && tar xf llvm-${LLVM_VERSION}.src.tar.xz
wget ${CLANG_URL} && tar xf clang-${LLVM_VERSION}.src.tar.xz && mv clang-${LLVM_VERSION}.src llvm-${LLVM_VERSION}.src/../clang

if [ $LLVM_MAJOR_VERSION -ge 15 ]; then
   wget ${LLVM_CMAKE_URL} && tar xf cmake-${LLVM_VERSION}.src.tar.xz && mv cmake-${LLVM_VERSION}.src cmake
fi

mkdir -p llvm-${LLVM_VERSION}.src/build && cd llvm-${LLVM_VERSION}.src/build

cmake -GNinja -DLLVM_ENABLE_RTTI=ON -DLLVM_ENABLE_EH=ON -DLLVM_ENABLE_PROJECTS="clang" -DLLVM_TARGETS_TO_BUILD="X86;AArch64" \
      -DCMAKE_BUILD_TYPE=Release -DLLVM_INCLUDE_TESTS=OFF -DLLVM_INCLUDE_BENCHMARKS=OFF  \
               -DCMAKE_INSTALL_PREFIX=${PREFIX}/llvm-${LLVM_VERSION} ..
ninja install
cd /tmp

echo ">> LLVM ${LLVM_VERSION} installation completed successfully"
