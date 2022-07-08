#!/usr/bin/env bash
# (c) L.Spiegelberg 2020
# install all dependencies required to compile tuplex + whatever is needed for profiling
# everything will be installed to /opt

set -ex

# Tuplex dependencies
apt-get update
apt-get install -y build-essential software-properties-common wget libedit-dev libz-dev python3-yaml python3-pip pkg-config libssl-dev libcurl4-openssl-dev curl uuid-dev git libffi-dev libmagic-dev doxygen doxygen-doc doxygen-latex doxygen-gui graphviz libgflags-dev libncurses-dev awscli openjdk-8-jdk libyaml-dev ninja-build gcc-10 g++-10 autoconf libtool m4 ssh unzip automake

# LLVM9 is broken on Ubuntu 20.04, hence manually install...

# add github to known hosts
mkdir -p /root/.ssh/ &&
  touch /root/.ssh/known_hosts &&
  ssh-keyscan github.com >>/root/.ssh/known_hosts

# fetch libraries from inet & compile
# install them all into /opt

# Yaml CPP (to read/write yaml option files)
mkdir -p /tmp/yaml-cpp-0.6.3 &&
  git clone https://github.com/jbeder/yaml-cpp.git /tmp/yaml-cpp &&
  cd /tmp/yaml-cpp &&
  git checkout tags/yaml-cpp-0.6.3 &&
  mkdir build &&
  cd build &&
  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt -DYAML_CPP_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS='-fPIC' .. &&
  make -j 32 &&
  make install &&
  cd &&
  rm -rf /tmp/yaml-cpp

# Celero (for benchmarking experiments)
mkdir -p /tmp/celero-v2.6.0 &&
  git clone https://github.com/DigitalInBlue/Celero.git /tmp/celero &&
  cd /tmp/celero &&
  git checkout tags/v2.6.0 &&
  mkdir build &&
  cd build &&
  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS=$CXX_FLAGS .. &&
  make -j 32 &&
  make install &&
  cd &&
  rm -rf /tmp/celero

# antlr4 + cpp runtime for it (for parsing python code)
curl -O https://www.antlr.org/download/antlr-4.8-complete.jar
mv antlr-4.8-complete.jar /opt/lib/

cd /tmp &&
  curl -O https://www.antlr.org/download/antlr4-cpp-runtime-4.8-source.zip &&
  unzip antlr4-cpp-runtime-4.8-source.zip -d antlr4-cpp-runtime &&
  rm antlr4-cpp-runtime-4.8-source.zip &&
  pushd antlr4-cpp-runtime &&
  mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt .. && make -j 32 && make install
  cd .. &&
  rm -rf antlr4-cpp-runtime &&
  cd - || echo "ANTLR4 runtime failed"

# AWS SDK
# tag 1.9.142?
# => note in 1.9.134/135 there has been a renaming of cJSON symbols -> this requires linking/renaming. cf. https://github.com/aws/aws-sdk-cpp/commit/2848c4571c94b03bc558378440f091f2017ef7d3
# note for centos7 there's an issue with SSL. Either use aws sdk with -DBUILD_DEPS=ON/-DUSE_OPENSSL=OFF. or force -DUSE_OPENSSL=ON.
cd /tmp &&
  git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git &&
  cd aws-sdk-cpp && git checkout tags/1.9.133 && mkdir build && pushd build &&
  cmake -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=14 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core;lambda;transfer" -DCMAKE_INSTALL_PREFIX=/opt .. &&
  make -j32 &&
  make install &&
  popd &&
  cd - || echo "AWS SDK failed"

# AWS Lambda cpp runtime
git clone https://github.com/awslabs/aws-lambda-cpp.git && \
  pushd aws-lambda-cpp && \
  git fetch && git fetch --tags && \
  git checkout v0.2.6 && \
  mkdir build && \
  cd build && \
  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt .. && \
  make -j 32 && make install && popd

# pcre2
cd /tmp &&
  curl -LO https://github.com/PhilipHazel/pcre2/releases/download/pcre2-10.39/pcre2-10.39.zip &&
  unzip pcre2-10.39.zip &&
  rm pcre2-10.39.zip &&
  pushd pcre2-10.39 &&
  ./configure --prefix=/opt --enable-jit=auto --disable-shared CFLAGS="-O2 -fPIC" && make -j 32 && make install
popd

# protobuf 3.19.3
cd /tmp &&
curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.19.3/protobuf-cpp-3.19.3.tar.gz &&
tar xf protobuf-cpp-3.19.3.tar.gz &&
pushd protobuf-3.19.3 &&
./autogen.sh && ./configure "CFLAGS=-fPIC" "CXXFLAGS=-fPIC" &&
make -j4 && make install && ldconfig &&
pushd

# setup bash aliases
echo "alias antlr='java -jar /opt/lib/antlr-4.8-complete.jar'" >>"$HOME/.bashrc"
echo "alias grun='java org.antlr.v4.gui.TestRig'" >>"$HOME/.bashrc"

# update include/link paths to /opt
echo "export CPLUS_INCLUDE_PATH=/opt/include:\${CPLUS_INCLUDE_PATH}" >> "$HOME/.bashrc"
echo "export C_INCLUDE_PATH=/opt/include:\${C_INCLUDE_PATH}" >> "$HOME/.bashrc"
echo "export LD_LIBRARY_PATH=/opt/lib:\${LD_LIBRARY_PATH}" >> "$HOME/.bashrc"
