 #!/usr/bin/env bash
# (c) L.Spiegelberg 2020
# (c) L.Spiegelberg 2020
# install all dependencies required to compile tuplex + whatever is needed for profiling
# everything will be installed to /opt

# need to run this with root privileges
if [[ $(id -u) -ne 0 ]]; then
  echo "Please run as root"
  exit 1
fi

# Tuplex dependencies
# compile dependencies
export DEBIAN_FRONTEND=noninteractive

# add recent python3.7 package, confer https://linuxize.com/post/how-to-install-python-3-7-on-ubuntu-18-04/
apt install -y software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa
apt-get update

apt-get install -y build-essential autoconf automake libtool software-properties-common wget libedit-dev libz-dev \
  python3-yaml pkg-config libssl-dev libcurl4-openssl-dev curl \
  uuid-dev git python3.7 python3.7-dev python3-pip libffi-dev \
  doxygen doxygen-doc doxygen-latex doxygen-gui graphviz \
  gcc-7 g++-7 libgflags-dev libncurses-dev \
  awscli openjdk-8-jdk libyaml-dev libmagic-dev ninja-build

# LLVM 9 packages (prob not all of them needed, but here for complete install)
wget https://apt.llvm.org/llvm.sh && chmod +x llvm.sh &&
./llvm.sh 9 && rm -rf llvm.sh


# @TODO: setup links for llvm tools in /usr/bin

# set gcc-7 as default
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 70 --slave /usr/bin/g++ g++ /usr/bin/g++-7
# set python3.7 as default
update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 70 --slave /usr/bin/python3m python3m /usr/bin/python3.7m
# upgrade pip
python3.7 -m pip install --upgrade pip

# fetch recent cmake & install
CMAKE_VER_MAJOR=3
CMAKE_VER_MINOR=23
CMAKE_VER_PATCH=2
CMAKE_VER="${CMAKE_VER_MAJOR}.${CMAKE_VER_MINOR}"
CMAKE_VERSION="${CMAKE_VER}.${CMAKE_VER_PATCH}"
URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz
mkdir -p /tmp/build && cd /tmp/build &&
  curl -sSL $URL -o cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  tar -v -zxf cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  rm -f cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  cd cmake-${CMAKE_VERSION}-linux-x86_64 &&
  cp -rp bin/* /usr/local/bin/ &&
  cp -rp share/* /usr/local/share/ &&
  cd / && rm -rf /tmp/build

# add github to known hosts
mkdir -p /root/.ssh/ &&
  touch /root/.ssh/known_hosts &&
  ssh-keyscan github.com >>/root/.ssh/known_hosts

# fetch libraries from inet & compile
# install them all into /opt
mkdir -p /opt && chmod 0755 /opt

# update python links
ln -s /usr/include/python3.7m /usr/include/python3.7 &&
  cd /tmp &&
  curl -OL https://gist.githubusercontent.com/LeonhardFS/a5cd056b5fe30ffb0b806f0383c880b3/raw/dfccad970434818f4c261c3bf1eed9daea5a9007/install_boost.py &&
  python2 install_boost.py --directory /tmp --prefix /opt --toolset gcc --address-model 64 --boost-version 1.70.0 --python python3 thread iostreams regex system filesystem python stacktrace atomic chrono date_time &&
  rm install_boost.py &&
  cd -

# needs gcc >= 6 and cmake >= 3.15

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
popd &&
  rm -rf antlr4-cpp-runtime &&
  cd -

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
  cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt -DENABLE_TESTS=OFF .. && \
  make -j 32 && make install && popd

# pcre2
cd /tmp &&
  curl -LO https://github.com/PhilipHazel/pcre2/releases/download/pcre2-10.39/pcre2-10.39.zip &&
  unzip pcre2-10.39.zip &&
  rm pcre2-10.39.zip &&
  pushd pcre2-10.39 &&
  ./configure --prefix=/opt --enable-jit=auto --disable-shared CFLAGS="-O2 -fPIC" && make -j 32 && make install
popd

# # protobuf 3.12 (all langs)
# cd /tmp &&
# git clone https://github.com/google/protobuf.git &&
# pushd protobuf && git fetch --tags &&
# git checkout v3.12.0 &&
# git submodule update --init --recursive &&
# ./autogen.sh && ./configure &&
# make && make check && make install && ldconfig &&
# popd
# cpp only version (builds faster)
cd /tmp &&
curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.12.0/protobuf-cpp-3.12.0.tar.gz &&
tar xf protobuf-cpp-3.12.0.tar.gz &&
pushd protobuf-3.12.0 &&
./autogen.sh && ./configure "CFLAGS=-fPIC" "CXXFLAGS=-fPIC" &&
make -j4 && make install && ldconfig &&
pushd

# install python packages for tuplex (needs cloudpickle to compile, numpy to run certain tests)
pip3 install 'cloudpickle<2.0.0' numpy

# setup bash aliases
echo "alias antlr='java -jar /opt/lib/antlr-4.8-complete.jar'" >>"$HOME/.bashrc"
echo "alias grun='java org.antlr.v4.gui.TestRig'" >>"$HOME/.bashrc"

# update include/link paths to /opt
echo "export CPLUS_INCLUDE_PATH=/opt/include:\${CPLUS_INCLUDE_PATH}" >> "$HOME/.bashrc"
echo "export C_INCLUDE_PATH=/opt/include:\${C_INCLUDE_PATH}" >> "$HOME/.bashrc"
echo "export LD_LIBRARY_PATH=/opt/lib:\${LD_LIBRARY_PATH}" >> "$HOME/.bashrc"
