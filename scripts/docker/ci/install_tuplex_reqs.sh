#!/usr/bin/env bash
#(c) 2017-2023 Tuplex team


# install all build dependencies for tuplex (CentOS)
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
yum install -y libedit-devel libzip-devel elfutils-devel   pkgconfig openssl-devel libxml2-devel zlib-devel    uuid libuuid-devel libffi-devel graphviz-devel   gflags-devel ncurses-devel   awscli java-1.8.0-openjdk-devel libyaml-devel file-devel ninja-build zip unzip ninja-build --skip-broken

# add github to known hosts
mkdir -p /root/.ssh/ &&
  touch /root/.ssh/known_hosts &&
  ssh-keyscan github.com >> /root/.ssh/known_hosts


echo ">> Installing YAMLCPP"
mkdir -p ${WORKDIR}/yamlcpp && cd ${WORKDIR}/yamlcpp \
&& git clone https://github.com/jbeder/yaml-cpp.git yaml-cpp \
&& cd yaml-cpp \
&& git checkout tags/yaml-cpp-0.6.3 \
&& mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${prefix} -DYAML_CPP_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC" .. \
&& make -j$(nproc) && make install

echo ">> Installing Celero"
mkdir -p ${WORKDIR}/celero && cd ${WORKDIR}/celero \
&&  git clone https://github.com/DigitalInBlue/Celero.git celero && cd celero \
&& git checkout tags/v2.8.3 \
&& mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC -std=c++11" .. \
&& make -j$(nproc) && make install

echo ">> Installing ANTLR"
mkdir -p ${WORKDIR}/antlr && cd ${WORKDIR}/antlr \
&& curl -O https://www.antlr.org/download/antlr-4.8-complete.jar \
&& cp antlr-4.8-complete.jar ${PREFIX}/lib/ \
&& curl -O https://www.antlr.org/download/antlr4-cpp-runtime-4.8-source.zip \
&& unzip antlr4-cpp-runtime-4.8-source.zip -d antlr4-cpp-runtime \
&& rm antlr4-cpp-runtime-4.8-source.zip \
&& cd antlr4-cpp-runtime \
&& mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} .. \
&& make -j$(nproc) && make install

echo ">> Installing AWS SDK"
mkdir -p ${WORKDIR}/aws && cd ${WORKDIR}/aws \
&&  git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git \
&& cd aws-sdk-cpp && git checkout tags/1.9.320 && mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=14 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core;lambda;transfer" -DCMAKE_INSTALL_PREFIX=${PREFIX} .. \
&& make -j$(nproc) \
&& make install

#installing AWS Lambda C++ runtime

cd ${WORKDIR}/aws \
&& git clone https://github.com/awslabs/aws-lambda-cpp.git \
&& cd aws-lambda-cpp \
&& git fetch && git fetch --tags \
&& git checkout v0.2.6 \
&& mkdir build \
&& cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} .. \
&& make -j$(nproc) && make install

echo ">> Installing PCRE2"
mkdir -p ${WORKDIR}/pcre2 && cd ${WORKDIR}/pcre2 \
&& curl -LO https://github.com/PhilipHazel/pcre2/releases/download/pcre2-10.39/pcre2-10.39.zip \
&& unzip pcre2-10.39.zip \
&& rm pcre2-10.39.zip \
&& cd pcre2-10.39 \
&& ./configure CFLAGS="-O2 -fPIC" --prefix=${PREFIX} --enable-jit=auto --disable-shared \
&& make -j$(nproc) && make install

echo ">> Installing protobuf"
mkdir -p ${WORKDIR}/protobuf && cd ${WORKDIR}/protobuf \
&& curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v21.5/protobuf-cpp-3.21.5.tar.gz \
&& tar xf protobuf-cpp-3.21.5.tar.gz \
&& cd protobuf-3.21.5 \
&& ./autogen.sh && ./configure "CFLAGS=-fPIC" "CXXFLAGS=-fPIC" \
&& make -j$(nproc) && make install && ldconfig