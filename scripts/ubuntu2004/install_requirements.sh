#!/usr/bin/env bash
# (c) Tuplex team 2017-2022
# auto-generated on 2022-09-13 11:53:43.862532
# install all dependencies required to compile tuplex + whatever is needed for profiling
# everything will be installed to /opt by default

# need to run this with root privileges
if [[ $(id -u) -ne 0 ]]; then
  echo "Please run this script with root privileges"
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

""" + workdir_setup() + """

PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python3}
PYTHON_BASENAME="$(basename -- $PYTHON_EXECUTABLE)"
PYTHON_VERSION=$(${PYTHON_EXECUTABLE} --version)
echo ">> Building dependencies for ${PYTHON_VERSION}"
echo ">> Installing all build dependencies for Tuplex under Ubuntu 20.04"

echo ">> Installing apt dependencies"
apt update -y

           apt-get install -y software-properties-common dh-autoreconf curl build-essential wget git libedit-dev libz-dev \
                   python3-yaml python3-pip pkg-config libssl-dev libcurl4-openssl-dev curl \
                   uuid-dev libffi-dev libmagic-dev \
                   doxygen doxygen-doc doxygen-latex doxygen-gui graphviz \
                   libgflags-dev libncurses-dev \
                   openjdk-8-jdk libyaml-dev ninja-build gcc-10 g++-10 autoconf libtool m4
                     
           
           ldconfig
           export CC=gcc-10
           export CXX=g++-10

echo ">> Installing recent cmake"
# fetch recent cmake & install
CMAKE_VER_MAJOR=3
CMAKE_VER_MINOR=23
CMAKE_VER_PATCH=3
CMAKE_VER="${CMAKE_VER_MAJOR}.${CMAKE_VER_MINOR}"
CMAKE_VERSION="${CMAKE_VER}.${CMAKE_VER_PATCH}"
URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz
mkdir -p ${WORKDIR}/cmake && cd ${WORKDIR}/cmake &&
  curl -sSL $URL -o cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  tar -v -zxf cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  rm -f cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  cd cmake-${CMAKE_VERSION}-linux-x86_64 &&
  cp -rp bin/* ${PREFIX}/bin/ &&
  cp -rp share/* ${PREFIX}/share/ &&
  cd / && rm -rf ${WORKDIR}/cmake

export PATH=$PREFIX/bin:$PATH
cmake --version


echo ">> Installing Boost"
mkdir -p ${WORKDIR}/boost

# build incl. boost python
pushd ${WORKDIR}/boost && wget https://boostorg.jfrog.io/artifactory/main/release/1.79.0/source/boost_1_79_0.tar.gz && tar xf boost_1_79_0.tar.gz && cd ${WORKDIR}/boost/boost_1_79_0 \
           && ./bootstrap.sh --with-python=${PYTHON_EXECUTABLE} --prefix=${PREFIX} --with-libraries="thread,iostreams,regex,system,filesystem,python,stacktrace,atomic,chrono,date_time" \
            && ./b2 cxxflags="-fPIC" link=static -j "$(nproc)" \
            && ./b2 cxxflags="-fPIC" link=static install && sed -i 's/#if PTHREAD_STACK_MIN > 0/#ifdef PTHREAD_STACK_MIN/g' ${PREFIX}/include/boost/thread/pthread/thread_data.hpp

echo ">> Installing LLVM"
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

echo ">> Installing PCRE2"
mkdir -p ${WORKDIR}/pcre2 && cd ${WORKDIR}/pcre2 \
&& curl -LO https://github.com/PhilipHazel/pcre2/releases/download/pcre2-10.39/pcre2-10.39.zip \
&& unzip pcre2-10.39.zip \
&& rm pcre2-10.39.zip \
&& cd pcre2-10.39 \
&& ./configure CFLAGS="-O2 -fPIC" --prefix=${PREFIX} --enable-jit=auto --disable-shared \
&& make -j$(nproc) && make install

echo ">> Installing Celero"
mkdir -p ${WORKDIR}/celero && cd ${WORKDIR}/celero \
&&  git clone https://github.com/DigitalInBlue/Celero.git celero && cd celero \
&& git checkout tags/v2.8.3 \
&& mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC -std=c++11" .. \
&& make -j$(nproc) && make install

echo ">> Installing YAMLCPP"
mkdir -p ${WORKDIR}/yamlcpp && cd ${WORKDIR}/yamlcpp \
&& git clone https://github.com/jbeder/yaml-cpp.git yaml-cpp \
&& cd yaml-cpp \
&& git checkout tags/yaml-cpp-0.6.3 \
&& mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${prefix} -DYAML_CPP_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC" .. \
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

echo ">> Installing protobuf"
mkdir -p ${WORKDIR}/protobuf && cd ${WORKDIR}/protobuf \
&& curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v21.5/protobuf-cpp-3.21.5.tar.gz \
&& tar xf protobuf-cpp-3.21.5.tar.gz \
&& cd protobuf-3.21.5 \
&& ./autogen.sh && ./configure "CFLAGS=-fPIC" "CXXFLAGS=-fPIC" \
&& make -j$(nproc) && make install && ldconfig

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

echo ">> Cleaning/removing workdir /tmp"
rm -rf ${WORKDIR}

echo "-- Done, all Tuplex requirements installed to /opt --"
