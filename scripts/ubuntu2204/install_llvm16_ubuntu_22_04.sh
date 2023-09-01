#!/usr/bin/env bash
# (c) Tuplex team 2017-2023
# install all dependencies required to compile tuplex + whatever is needed for profiling
# everything will be installed to /opt by default

## need to run this with root privileges
#if [[ $(id -u) -ne 0 ]]; then
#  echo "Please run this script with root privileges"
#  exit 1
#fi

export DEBIAN_FRONTEND=noninteractive

PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}

#echo ">> Installing packages into ${PREFIX}"
#mkdir -p $PREFIX && chmod 0755 $PREFIX
#mkdir -p $PREFIX/sbin
#mkdir -p $PREFIX/bin
#mkdir -p $PREFIX/share
#mkdir -p $PREFIX/include
#mkdir -p $PREFIX/lib

echo ">> Files will be downloaded to ${WORKDIR}/tuplex-downloads"
WORKDIR=$WORKDIR/tuplex-downloads
mkdir -p $WORKDIR

LLVM_VERSION=16.0.6
LLVM_MAJOR_VERSION=${LLVM_VERSION%%.*}

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
wget ${CLANG_URL} && tar xf clang-${LLVM_VERSION}.src.tar.xz && mv clang-${LLVM_VERSION}.src llvm-${LLVM_VERSION}.src/clang

if (( LLVM_MAJOR_VERSION >= 15 )); then
  wget ${LLVM_CMAKE_URL} && tar xf cmake-${LLVM_VERSION}.src.tar.xz && mv cmake-${LLVM_VERSION}.src llvm-${LLVM_VERSION}.src/cmake
fi


cmake -DLLVM_ENABLE_RTTI=ON -DLLVM_ENABLE_EH=ON \
        -DLLVM_ENABLE_PROJECTS="clang" \
         -DLLVM_TARGETS_TO_BUILD="X86;AArch64" -DCMAKE_BUILD_TYPE=Release -DLLVM_INCLUDE_TESTS=OFF -DLLVM_INCLUDE_BENCHMARKS=OFF -DCMAKE_CXX_FLAGS="-std=c++11 -include /usr/include/c++/11/limits" \
         -DCMAKE_INSTALL_PREFIX=/opt/llvm-${LLVM_VERSION} llvm-${LLVM_VERSION}.src

popd


#echo ">> Installing LLVM"
#mkdir -p ${WORKDIR}/llvm && cd ${WORKDIR}/llvm && wget ${LLVM_URL} \
#&& wget ${CLANG_URL} \
#&& tar xf llvm-${LLVM_VERSION}.src.tar.xz && tar xf clang-${LLVM_VERSION}.src.tar.xz \
#&& mkdir llvm${LLVM_VERSION} && mv clang-${LLVM_VERSION}.src llvm${LLVM_VERSION}/clang && mv cmake llvm${LLVM_VERSION}/cmake \
#    && mv llvm-${LLVM_VERSION}.src llvm${LLVM_VERSION}/llvm-${LLVM_VERSION}.src \
#    && cd llvm${LLVM_VERSION} && mkdir build && cd build \
#&& cmake -DLLVM_ENABLE_RTTI=ON -DLLVM_ENABLE_EH=ON \
#        -DLLVM_ENABLE_PROJECTS="clang" \
#         -DLLVM_TARGETS_TO_BUILD="X86" -DCMAKE_BUILD_TYPE=Release -DLLVM_INCLUDE_BENCHMARKS=OFF -DCMAKE_CXX_FLAGS="-std=c++11 -include /usr/include/c++/11/limits" \
#         -DCMAKE_INSTALL_PREFIX=/opt/llvm-${LLVM_VERSION} ../llvm-${LLVM_VERSION}.src \
#&& make -j "$(nproc)"# && make install
#
#$echo ">> Cleaning/removing workdir /tmp"
#rm -rf ${WORKDIR}
#
#echo "-- Done, all Tuplex requirements installed to /opt --"
