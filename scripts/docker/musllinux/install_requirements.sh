#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs all tuplex dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python3}
CMAKE_VERSION="3.27.5"
BOOST_VERSION="1.88.0"
LLVM_VERSION="16.0.6"
AWSSDK_CPP_VERSION=1.11.524
ANTLR4_VERSION=4.13.1
YAML_CPP_VERSION=0.8.0
AWS_LAMBDA_CPP_VERSION=0.2.10
PCRE2_VERSION=10.45
PROTOBUF_VERSION=32.0
CELERO_VERSION=2.8.3
SNAPPY_VERSION=1.2.2
CC=gcc
CXX=g++

CPU_COUNT=$(( 1 * $( grep '^processor[[:space:]]*:' /proc/cpuinfo | wc -l ) ))

PYTHON_VERSION=$(echo $(python3 --version) | cut -d ' ' -f2)
PYTHON_MAJMIN_VERSION=${PYTHON_VERSION%.*}
echo ">> Installing dependencies for Python version ${PYTHON_VERSION}"

version() { echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }'; }

# Start script.
set -euxo pipefail

# need to run this with root privileges
if [ "$(id -u)" -ne 0 ]; then
  echo "Please run this script with root privileges"
  exit 1
fi

# --- helper functions ---
# Parse the major, minor and patch versions.
# You use it like this:
#    semver="3.4.5+xyz"
#    a=($(parse_semver "$semver"))
#    major=${a[0]}
#    minor=${a[1]}
#    patch=${a[2]}
#    printf "%-32s %4d %4d %4d\n" "$semver" $major $minor $patch
parse_semver() {
    token="$1"
    major=0
    minor=0
    patch=0

    if echo "$token" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' >/dev/null 2>&1 ; then
        # It has the correct syntax.
        n=$(echo "$token" | tr -cd '0-9.' | tr '.' ' ')
        major=$(echo "$n" | cut -d' ' -f1)
        minor=$(echo "$n" | cut -d' ' -f2)
        patch=$(echo "$n" | cut -d' ' -f3)
    fi

    echo "$major $minor $patch"
}

install_llvm() {
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
   LLVM_WORKDIR=${DOWNLOAD_DIR}/llvm${LLVM_VERSION}
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
         -DCMAKE_INSTALL_PREFIX=/opt/llvm-${LLVM_VERSION} ..
   ninja install
  cd /tmp
}

echo ">> Installing packages into ${PREFIX}"
mkdir -p $PREFIX && chmod 0755 $PREFIX
mkdir -p $PREFIX/sbin
mkdir -p $PREFIX/bin
mkdir -p $PREFIX/share
mkdir -p $PREFIX/include
mkdir -p $PREFIX/lib

echo ">> Files will be downloaded to ${WORKDIR}/tuplex-downloads"
DOWNLOAD_DIR=$WORKDIR/tuplex-downloads
mkdir -p $DOWNLOAD_DIR

PYTHON_BASENAME="$(basename -- $PYTHON_EXECUTABLE)"
PYTHON_VERSION=$(${PYTHON_EXECUTABLE} --version)
echo ">> Building dependencies for ${PYTHON_VERSION}"
echo ">> Installing all build dependencies for Tuplex under Alpine Linux"

echo ">> Installing apk dependencies"
apk update

apk add --no-cache \
    autoconf automake libtool \
    curl libxml2-dev vim build-base \
    openssl-dev zlib-dev ncurses-dev \
    readline-dev sqlite-dev \
    bzip2-dev expat-dev xz-dev \
    tk-dev libffi-dev wget git \
    curl-dev python3-dev py3-pip \
    openjdk11 ninja \
    linux-headers musl-dev \
    openssh file-dev

export CC=${CC}
export CXX=${CXX}

echo ">> Installing recent cmake"
# fetch recent cmake & install - detect architecture
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    CMAKE_ARCH="aarch64"
else
    CMAKE_ARCH="x86_64"
fi
URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz
mkdir -p ${DOWNLOAD_DIR}/cmake && cd ${DOWNLOAD_DIR}/cmake &&
  curl -sSL $URL -o cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz &&
  tar -v -zxf cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz &&
  rm -f cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz &&
  cd cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH} &&
  cp -rp bin/* ${PREFIX}/bin/ &&
  cp -rp share/* ${PREFIX}/share/ &&
  cd / && rm -rf ${DOWNLOAD_DIR}/cmake

export PATH=$PREFIX/bin:$PATH
cmake --version


echo ">> Installing Boost"
install_boost() {
    local boost_version="${1:-$BOOST_VERSION}"
    local download_dir="${2:-$DOWNLOAD_DIR}"
    local prefix="${3:-$PREFIX}"
    local python_executable="${4:-$PYTHON_EXECUTABLE}"
    local cpu_count="${5:-$CPU_COUNT}"

    echo ">> Installing Boost ${boost_version}"
    mkdir -p "${download_dir}/boost"

    # create underscored version, e.g. 1.79.0 -> 1_79_0
    local boost_underscored_version
    boost_underscored_version=$(echo "${boost_version}" | tr . _)

    # Download and build Boost including boost python
    cd "${download_dir}/boost"
    curl -L -O "https://github.com/boostorg/boost/releases/download/boost-${boost_version}/boost-${boost_version}-b2-nodocs.tar.gz"
    tar xf "boost-${boost_version}-b2-nodocs.tar.gz"
    cd "${download_dir}/boost/boost-${boost_version}"
    ./bootstrap.sh --with-python="${python_executable}" --prefix="${prefix}" --with-libraries="thread,iostreams,regex,system,filesystem,python,stacktrace,atomic,chrono,date_time"
    ./b2 cxxflags="-fPIC" link=static -j "${cpu_count}"
    ./b2 cxxflags="-fPIC" link=static install
    sed -i 's/#if PTHREAD_STACK_MIN > 0/#ifdef PTHREAD_STACK_MIN/g' "${prefix}/include/boost/thread/pthread/thread_data.hpp"

    cd "${download_dir}"
    rm -rf "${download_dir}/boost"
}

install_boost "$BOOST_VERSION" "$DOWNLOAD_DIR" "$PREFIX" "$PYTHON_EXECUTABLE" "$CPU_COUNT"

## -- install llvm
install_llvm $LLVM_VERSION


echo ">>> Installing tuplex dependencies."
# install recent zlib version (1.2.11) fork from cloudflare
# https://github.com/aws/aws-graviton-getting-started#zlib-on-linux
LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}
export LD_LIBRARY_PATH=$PREFIX/lib:$PREFIX/lib64:$LD_LIBRARY_PATH

# Cloudflare fork is too old
#mkdir -p $WORKDIR/zlib && cd $WORKDIR && git clone https://github.com/cloudflare/zlib.git && cd zlib && ./configure --prefix=$PREFIX && make -j ${CPU_COUNT} && make install

# note that zlib defines Z_NULL=0 whereas zlib-ng defines it as NULL, patch aws sdk accordingly
git clone https://github.com/zlib-ng/zlib-ng.git && cd zlib-ng && git checkout tags/2.1.3 && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" -DZLIB_COMPAT=ON .. && make -j ${CPU_COUNT} && make install

git clone https://github.com/google/googletest.git -b v1.14.0 && cd googletest && mkdir build && cd build && cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="-fPIC" -DCMAKE_BUILD_TYPE=Release .. && make -j ${CPU_COUNT} && make install

# build snappy as static lib
git clone https://github.com/google/snappy.git -b ${SNAPPY_VERSION} && cd snappy && git submodule update --init && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC" .. && make -j ${CPU_COUNT} && make install

# SSH setup not needed for Docker build - ssh-keyscan not available
mkdir -p /root/.ssh/ &&
  touch /root/.ssh/known_hosts &&
  ssh-keyscan github.com >> /root/.ssh/known_hosts


echo ">> Installing YAMLCPP"
mkdir -p ${DOWNLOAD_DIR}/yamlcpp && cd ${DOWNLOAD_DIR}/yamlcpp \
&& git clone https://github.com/jbeder/yaml-cpp.git yaml-cpp \
&& cd yaml-cpp \
&& git checkout tags/${YAML_CPP_VERSION} \
&& mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} -DYAML_CPP_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC" .. \
&& make -j ${CPU_COUNT} && make install

echo ">> Installing Celero"
mkdir -p ${DOWNLOAD_DIR}/celero && cd ${DOWNLOAD_DIR}/celero \
&&  git clone https://github.com/DigitalInBlue/Celero.git celero && cd celero \
&& git checkout tags/v${CELERO_VERSION} \
&& mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC -std=c++11" .. \
&& make -j ${CPU_COUNT} && make install

echo ">> Installing ANTLR"
mkdir -p ${DOWNLOAD_DIR}/antlr && cd ${DOWNLOAD_DIR}/antlr \
&& curl -O https://www.antlr.org/download/antlr-${ANTLR4_VERSION}-complete.jar \
&& cp antlr-${ANTLR4_VERSION}-complete.jar ${PREFIX}/lib/ \
&& curl -O https://www.antlr.org/download/antlr4-cpp-runtime-${ANTLR4_VERSION}-source.zip \
&& unzip antlr4-cpp-runtime-${ANTLR4_VERSION}-source.zip -d antlr4-cpp-runtime \
&& rm antlr4-cpp-runtime-${ANTLR4_VERSION}-source.zip \
&& cd antlr4-cpp-runtime \
&& mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} .. \
&& make -j ${CPU_COUNT}&& make install

echo ">> Installing AWS SDK"
# Note the z-lib patch here.
mkdir -p ${DOWNLOAD_DIR}/aws && cd ${DOWNLOAD_DIR}/aws \
&& git clone --progress --verbose --recurse-submodules https://github.com/aws/aws-sdk-cpp.git \
&& cd aws-sdk-cpp && git checkout tags/${AWSSDK_CPP_VERSION} && sed -i 's/int ret = Z_NULL;/int ret = static_cast<int>(Z_NULL);/g' src/aws-cpp-sdk-core/source/client/RequestCompression.cpp && mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DUSE_CRT_HTTP_CLIENT=ON -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=17 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;s3-crt;core;lambda;transfer" -DCMAKE_INSTALL_PREFIX=${PREFIX} .. \
&& make -j ${CPU_COUNT} \
&& make install

# Installing AWS Lambda C++ runtime.
cd ${DOWNLOAD_DIR}/aws \
&& git clone https://github.com/awslabs/aws-lambda-cpp.git \
&& cd aws-lambda-cpp \
&& git fetch && git fetch --tags \
&& git checkout v${AWS_LAMBDA_CPP_VERSION} \
&& mkdir build \
&& cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} .. \
&& make -j${CPU_COUNT} && make install

echo ">> Installing PCRE2"
mkdir -p ${DOWNLOAD_DIR}/pcre2 && cd ${DOWNLOAD_DIR}/pcre2 \
&& curl -LO https://github.com/PhilipHazel/pcre2/releases/download/pcre2-${PCRE2_VERSION}/pcre2-${PCRE2_VERSION}.zip \
&& unzip pcre2-${PCRE2_VERSION}.zip \
&& rm pcre2-${PCRE2_VERSION}.zip \
&& cd pcre2-${PCRE2_VERSION} \
&& ./configure CFLAGS="-O2 -fPIC" --prefix=${PREFIX} --enable-jit=auto --disable-shared \
&& make -j${CPU_COUNT} && make install

echo ">> Installing protobuf"
mkdir -p ${DOWNLOAD_DIR}/protobuf && cd ${DOWNLOAD_DIR}/protobuf \
&& git clone -b v${PROTOBUF_VERSION} https://github.com/protocolbuffers/protobuf.git && cd protobuf && git submodule update --init --recursive && mkdir build && cd build && cmake -DCMAKE_CXX_FLAGS="-fPIC" -DCMAKE_CXX_STANDARD=17 -Dprotobuf_BUILD_TESTS=OFF .. && make -j ${CPU_COUNT} && make install


# delete workdir (downloads dir) to clean up space
rm -rf ${DOWNLOAD_DIR}

echo "-- Done, all Tuplex requirements installed to /opt --"

# Verify that key tools are available
echo ">> Verifying installation..."
ls -la /opt/bin/
echo ">> PATH: $PATH"
echo ">> CMake version:"
/opt/bin/cmake --version || echo "CMake not found in /opt/bin"
echo ">> Python version:"
python3 --version || echo "Python3 not found"
