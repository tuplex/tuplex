#!/usr/bin/env bash
# installs LLVM 9.0.1 to /opt because apt repositories from llvm are broken -.-

# apt-get update && apt-get install -y libxml2-dev
yum update && yum install -y wget libxml2-devel

cd /usr/src &&
wget https://github.com/llvm/llvm-project/releases/download/llvmorg-9.0.1/llvm-9.0.1.src.tar.xz &&
wget https://github.com/llvm/llvm-project/releases/download/llvmorg-9.0.1/clang-9.0.1.src.tar.xz &&
tar xf llvm-9.0.1.src.tar.xz && tar xf clang-9.0.1.src.tar.xz

mkdir -p llvm9

# move compiler-rt and clang into nested dirs
mv clang-9.0.1.src llvm9/clang
mv llvm-9.0.1.src llvm9/llvm-9.0.1.src
cd llvm9

mkdir -p build && cd build \
&& cmake -DLLVM_ENABLE_RTTI=ON -DLLVM_ENABLE_EH=ON \
        -DLLVM_ENABLE_PROJECTS="clang" \
         -DLLVM_TARGETS_TO_BUILD="X86" -DCMAKE_BUILD_TYPE=Release \
         -DCMAKE_INSTALL_PREFIX=/opt/llvm-9.0 ../llvm-9.0.1.src \
&& make -j "$(nproc)"

make install
cd /opt/llvm-9.0/bin && ln -s clang++ clang++-9.0

echo "LLVM9 installed"
