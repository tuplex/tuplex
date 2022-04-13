#!/usr/bin/env bash
# installs LLVM 6.0.1 to /opt

apt-get update && apt-get install -y libxml2-dev wget build-essential

cd /usr/src &&
wget https://releases.llvm.org/6.0.1/llvm-6.0.1.src.tar.xz &&
wget https://releases.llvm.org/6.0.1/cfe-6.0.1.src.tar.xz &&
tar xf llvm-6.0.1.src.tar.xz && tar xf cfe-6.0.1.src.tar.xz

# move compiler-rt and clang into nested dirs
mv cfe-6.0.1.src clang

mkdir build && cd build \
&& cmake -DLLVM_ENABLE_RTTI=ON -DLLVM_ENABLE_EH=ON \
        -DLLVM_ENABLE_PROJECTS="clang" \
         -DLLVM_TARGETS_TO_BUILD="X86" -DCMAKE_BUILD_TYPE=Release \
         -DCMAKE_INSTALL_PREFIX=/opt/llvm-6.0 ../llvm-6.0.1.src \
&& make -j "$(nproc)"

make install

# fix link for clang for weld
# ln -s /opt/llvm-6.0/bin/clang++ /opt/llvm-6.0/bin/clang++-6.0
cd /opt/llvm-6.0/bin && ln -s clang++ clang++-6.0

echo "LLVM6 installed"
