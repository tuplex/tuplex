#!/usr/bin/env bash
#(c) 2017-2022 Tuplex team


# install LLVM 9.0.1 to use for building wheels

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

yum update && yum install -y wget libxml2-devel
mkdir -p ${WORKDIR}/llvm && cd ${WORKDIR}/llvm && wget https://github.com/llvm/llvm-project/releases/download/llvmorg-9.0.1/llvm-9.0.1.src.tar.xz \
&& wget https://github.com/llvm/llvm-project/releases/download/llvmorg-9.0.1/clang-9.0.1.src.tar.xz \
&& tar xf llvm-9.0.1.src.tar.xz && tar xf clang-9.0.1.src.tar.xz \
&& mkdir llvm9 && mv clang-9.0.1.src llvm9/clang \
    && mv llvm-9.0.1.src llvm9/llvm-9.0.1.src \
    && cd llvm9 && mkdir build && cd build \
&& cmake -DLLVM_ENABLE_RTTI=ON -DLLVM_ENABLE_EH=ON \
        -DLLVM_ENABLE_PROJECTS="clang" \
         -DLLVM_TARGETS_TO_BUILD="X86" -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-std=c++11" \
         -DCMAKE_INSTALL_PREFIX=/opt/llvm-9.0.1 ../llvm-9.0.1.src \
&& make -j "$(nproc)" && make install
cd ${PREFIX}/llvm-9.0/bin && ln -s clang++ clang++-9.0