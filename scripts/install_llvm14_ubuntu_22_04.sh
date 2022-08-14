#!/usr/bin/env bash
# (c) Tuplex team 2017-2022
# auto-generated on 2022-08-14 17:42:10.414990
# install all dependencies required to compile tuplex + whatever is needed for profiling
# everything will be installed to /opt by default

# need to run this with root privileges
if [[ $(id -u) -ne 0 ]]; then
  echo "Please run this script with root privileges"
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

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

PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python3}
PYTHON_BASENAME="$(basename -- $PYTHON_EXECUTABLE)"
PYTHON_VERSION=$(${PYTHON_EXECUTABLE} --version)
echo ">> Building dependencies for ${PYTHON_VERSION}"

echo ">> Installing LLVM"
mkdir -p ${WORKDIR}/llvm && cd ${WORKDIR}/llvm && wget https://github.com/llvm/llvm-project/releases/download/llvmorg-14.0.6/llvm-14.0.6.src.tar.xz \
&& wget https://github.com/llvm/llvm-project/releases/download/llvmorg-14.0.6/clang-14.0.6.src.tar.xz \
&& tar xf llvm-14.0.6.src.tar.xz && tar xf clang-14.0.6.src.tar.xz \
&& mkdir llvm14 && mv clang-14.0.6.src llvm14/clang && mv cmake llvm14/cmake \
    && mv llvm-14.0.6.src llvm14/llvm-14.0.6.src \
    && cd llvm14 && mkdir build && cd build \
&& cmake -DLLVM_ENABLE_RTTI=ON -DLLVM_ENABLE_EH=ON \
        -DLLVM_ENABLE_PROJECTS="clang" \
         -DLLVM_TARGETS_TO_BUILD="X86" -DCMAKE_BUILD_TYPE=Release -DLLVM_INCLUDE_BENCHMARKS=OFF -DCMAKE_CXX_FLAGS="-std=c++11 -include /usr/include/c++/11/limits" \
         -DCMAKE_INSTALL_PREFIX=/opt/llvm-14.0.6 ../llvm-14.0.6.src \
&& make -j "$(nproc)" && make install

echo ">> Cleaning/removing workdir /tmp"
rm -rf ${WORKDIR}

echo "-- Done, all Tuplex requirements installed to /opt --"
