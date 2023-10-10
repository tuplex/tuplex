#!/usr/bin/env bash
#(c) 2017-2023 Tuplex team

set -euxo pipefail

# dependency versions
AWSSDK_CPP_VERSION=1.11.164
ANTLR4_VERSION=4.13.1
YAML_CPP_VERSION=0.8.0
AWS_LAMBDA_CPP_VERSION=0.2.8
PCRE2_VERSION=10.42
PROTOBUF_VERSION=24.3

PYTHON_VERSION=$(echo $(python3 --version) | cut -d ' ' -f2)
PYTHON_MAJMIN_VERSION=${PYTHON_VERSION%%.[0-9]}
echo ">> Installing dependencies for Python version ${PYTHON_VERSION}"

function version { echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }'; }

# install python dependencies depending on version
declare -A PYTHON_DEPENDENCIES=(["3.8"]="cloudpickle<2.0 cython numpy pandas" ["3.9"]="cloudpickle<2.0 numpy pandas" ["3.10"]="cloudpickle>2.0 numpy pandas" ["3.11"]="cloudpickle>2.0 numpy pandas")
PYTHON_REQUIREMENTS=$(echo "${PYTHON_DEPENDENCIES[$PYTHON_MAJMIN_VERSION]}")
python3 -m pip install ${PYTHON_REQUIREMENTS}

# install all build dependencies for tuplex (CentOS)
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
CPU_COUNT=$(( 1 * $( egrep '^processor[[:space:]]+:' /proc/cpuinfo | wc -l ) ))

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
yum install -y libedit-devel libzip-devel pkgconfig libxml2-devel uuid libuuid-devel libffi-devel graphviz-devel gflags-devel ncurses-devel   awscli java-11-openjdk libyaml-devel file-devel ninja-build zip unzip ninja-build --skip-broken

# if java exists, remove via
yum remove -y java-1.8.0-openjdk-headless

# install recent zlib version (1.2.11) fork from cloudflare
# https://github.com/aws/aws-graviton-getting-started#zlib-on-linux
export LD_LIBRARY_PATH=$PREFIX/lib:$PREFIX/lib64:$LD_LIBRARY_PATH

# Cloudflare fork is too old
#mkdir -p $WORKDIR/zlib && cd $WORKDIR && git clone https://github.com/cloudflare/zlib.git && cd zlib && ./configure --prefix=$PREFIX && make -j ${CPU_COUNT} && make install

# note that zlib defines Z_NULL=0 whereas zlib-ng defines it as NULL, patch aws sdk accordingly
git clone https://github.com/zlib-ng/zlib-ng.git && cd zlib-ng && git checkout tags/2.1.3 && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" -DZLIB_COMPAT=ON .. && make -j ${CPU_COUNT} && make install

git clone https://github.com/google/googletest.git -b v1.14.0 && cd googletest && mkdir build && cd build && cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="-fPIC" -DCMAKE_BUILD_TYPE=Release .. && make -j ${CPU_COUNT} && make install

# build snappy as static lib
git clone https://github.com/google/snappy.git -b 1.1.10 && cd snappy && git submodule update --init && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC" .. && make -j ${CPU_COUNT} && make install

# custom OpenSSL, use a recent OpenSSL and uninstall current one
if which yum; then
	yum erase -y openssl-devel openssl
else
	apk del openssl-dev openssl
fi
cd $WORKDIR && \
  wget https://ftp.openssl.org/source/openssl-1.1.1w.tar.gz && \
  tar -xzvf openssl-1.1.1w.tar.gz && \
  cd openssl-1.1.1w && \
  ./config no-shared zlib-dynamic CFLAGS="-fPIC" CXXFLAGS="-fPIC" LDFLAGS="-fPIC" && \
  make -j ${CPU_COUNT} && make install_sw && echo "OpenSSL ok"
# this will install openssl into /usr/local

# add github to known hosts
mkdir -p /root/.ssh/ &&
  touch /root/.ssh/known_hosts &&
  ssh-keyscan github.com >> /root/.ssh/known_hosts


echo ">> Installing YAMLCPP"
mkdir -p ${WORKDIR}/yamlcpp && cd ${WORKDIR}/yamlcpp \
&& git clone https://github.com/jbeder/yaml-cpp.git yaml-cpp \
&& cd yaml-cpp \
&& git checkout tags/${YAML_CPP_VERSION} \
&& mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} -DYAML_CPP_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC" .. \
&& make -j ${CPU_COUNT} && make install

echo ">> Installing Celero"
mkdir -p ${WORKDIR}/celero && cd ${WORKDIR}/celero \
&&  git clone https://github.com/DigitalInBlue/Celero.git celero && cd celero \
&& git checkout tags/v2.8.3 \
&& mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC -std=c++11" .. \
&& make -j ${CPU_COUNT} && make install

echo ">> Installing ANTLR"
mkdir -p ${WORKDIR}/antlr && cd ${WORKDIR}/antlr \
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
mkdir -p ${WORKDIR}/aws && cd ${WORKDIR}/aws \
&& git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git \
&& cd aws-sdk-cpp && git checkout tags/${AWSSDK_CPP_VERSION} && sed -i 's/int ret = Z_NULL;/int ret = static_cast<int>(Z_NULL);/g' src/aws-cpp-sdk-core/source/client/RequestCompression.cpp && mkdir build && cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=17 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core;lambda;transfer" -DCMAKE_INSTALL_PREFIX=${PREFIX} .. \
&& make -j ${CPU_COUNT} \
&& make install

#installing AWS Lambda C++ runtime
cd ${WORKDIR}/aws \
&& git clone https://github.com/awslabs/aws-lambda-cpp.git \
&& cd aws-lambda-cpp \
&& git fetch && git fetch --tags \
&& git checkout v${AWS_LAMBDA_CPP_VERSION} \
&& mkdir build \
&& cd build \
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} .. \
&& make -j$(nproc) && make install

echo ">> Installing PCRE2"
mkdir -p ${WORKDIR}/pcre2 && cd ${WORKDIR}/pcre2 \
&& curl -LO https://github.com/PhilipHazel/pcre2/releases/download/pcre2-${PCRE2_VERSION}/pcre2-${PCRE2_VERSION}.zip \
&& unzip pcre2-${PCRE2_VERSION}.zip \
&& rm pcre2-${PCRE2_VERSION}.zip \
&& cd pcre2-${PCRE2_VERSION} \
&& ./configure CFLAGS="-O2 -fPIC" --prefix=${PREFIX} --enable-jit=auto --disable-shared \
&& make -j$(nproc) && make install

echo ">> Installing protobuf"
mkdir -p ${WORKDIR}/protobuf && cd ${WORKDIR}/protobuf \
&& git clone -b v${PROTOBUF_VERSION} https://github.com/protocolbuffers/protobuf.git && cd protobuf && git submodule update --init --recursive && mkdir build && cd build && cmake -DCMAKE_CXX_FLAGS="-fPIC" -DCMAKE_CXX_STANDARD=17 -Dprotobuf_BUILD_TESTS=OFF .. && make -j ${CPU_COUNT} && make install


# delete workdir (downloads dir) to clean up space
rm -rf ${WORKDIR}
yum clean all
