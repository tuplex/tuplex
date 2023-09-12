#!/usr/bin/env bash
#(c) 2017-2022 Tuplex team

set -euxo pipefail

# install LLVM 9.0.1 to use for building wheels
LLVM_VERSIONS_TO_INSTALL=(9.0.1)

function install_llvm {
   LLVM_VERSION=$1
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

   echo ">> Downloading prerequisites for llvm ${LLVM_VERSION}}"
   LLVM_WORKDIR=${WORKDIR}/llvm${LLVM_VERSION}
   mkdir -p ${LLVM_WORKDIR}
   pushd "${LLVM_WORKDIR}" || exit 1

   wget ${LLVM_URL} && tar xf llvm-${LLVM_VERSION}.src.tar.xz
   wget ${CLANG_URL} && tar xf clang-${LLVM_VERSION}.src.tar.xz && mv clang-${LLVM_VERSION}.src llvm-${LLVM_VERSION}.src/../clang

   if (( LLVM_MAJOR_VERSION >= 15 )); then
      wget ${LLVM_CMAKE_URL} && tar xf cmake-${LLVM_VERSION}.src.tar.xz && mv cmake-${LLVM_VERSION}.src llvm-${LLVM_VERSION}.src/cmake
   fi


   cmake -GNinja -DLLVM_ENABLE_RTTI=ON -DLLVM_ENABLE_EH=ON \
         -DLLVM_ENABLE_PROJECTS="clang" \
         -DLLVM_TARGETS_TO_BUILD="X86;AArch64" -DCMAKE_BUILD_TYPE=Release -DLLVM_INCLUDE_TESTS=OFF -DLLVM_INCLUDE_BENCHMARKS=OFF  \
         -DCMAKE_INSTALL_PREFIX=/opt/llvm-${LLVM_VERSION} llvm-${LLVM_VERSION}.src
   ninja install
  popd
}


PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}

echo ">> Installing packages into ${PREFIX}"
mkdir -p $PREFIX && chmod 0755 $PREFIX
mkdir -p $PREFIX/sbin
mkdir -p $PREFIX/bin
mkdir -p $PREFIX/share
mkdir -p $PREFIX/include
mkdir -p $PREFIX/lib

echo ">> Files will be downloaded to ${WORKDIR}/tuplex-downloads"
WORKDIR=$WORKDIR/tuplex-downloads
mkdir -p $WORKDIR

for llvm_version in "${LLVM_VERSIONS_TO_INSTALL[@]}"; do
  echo "Installing LLVM ${llvm_version}"
  install_llvm ${llvm_version}
done

echo "done with LLVM install"
