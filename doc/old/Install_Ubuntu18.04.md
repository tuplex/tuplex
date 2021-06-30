# install dependencies Ubuntu 18.04

mkdir -p /opt/cmake
mkdir -p /opt/antlr

# install cmake
wget https://cmake.org/files/v3.15/cmake-3.15.3-Linux-x86_64.sh cmake-3.15.3-Linux-x86_64.sh && \
sh cmake-3.15.3-Linux-x86_64.sh --prefix=/opt/cmake --skip-license && \
ln -s /opt/cmake/bin/cmake /usr/local/bin/cmake && \
cmake --version

# install additional dependencies via apt-get (requires ~1.5GB space)
apt-get install -y build-essential software-properties-common wget libedit-dev libz-dev valgrind python-yaml pkg-config libssl-dev libcurl4-openssl-dev curl openjdk-8-jdk uuid-dev python3-dev python3-pip libffi-dev doxygen doxygen-doc doxygen-latex doxygen-gui graphviz doxygen doxygen-doc doxygen-latex doxygen-gui graphviz libncurses5-dev gcc-6 g++-6

# install LLVM 5
bash -c 'wget -O - http://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - &&
add-apt-repository -y "deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-5.0 main" &&
apt-get update &&
apt-get install -y clang-5.0 lldb-5.0 lld-5.0'


# update alternatives for clang + gcc if desired
update-alternatives --install /usr/bin/clang clang-5.0 /usr/bin/clang-5.0 100 --slave /usr/bin/clang++ clang++ /usr/bin/clang++-5.0
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-6 60 --slave /usr/bin/g++ g++ /usr/bin/g++-6

# compile dependencies
export CXX_FLAGS="-Wall -pedantic -Werror -Wno-variadic-macros -Wno-long-long -Wno-shadow -fPIC"


# ANTLR4
bash -c 'wget https://www.antlr.org/download/antlr-4.7.2-complete.jar &&
mv antlr-4.7.2-complete.jar /opt/antlr/'
# add antlr4 aliases if desired
echo "alias antlr='java -jar /opt/antlr/antlr-4.7.2-complete.jar'" >> $HOME/.bashrc
echo "alias grun='java org.antlr.v4.gui.TestRig'" >> $HOME/.bashrc



# install boost 1.71
bash -c 'pushd /tmp &&
curl -Lo boost.tar.gz https://dl.bintray.com/boostorg/release/1.71.0/source/boost_1_71_0.tar.gz &&
tar xf boost.tar.gz &&
cd boost_1_71_0 && ./bootstrap.sh --with-python=python3 &&
./b2 install cxxflags=-fPIC toolset=gcc-6 link=static runtime-link=shared variant=release address-model=64 define=NO_COMPRESSION=1 --prefix=/usr/local &&
popd'

# ANTLR4Runtime
bash -c 'curl -O https://www.antlr.org/download/antlr4-cpp-runtime-4.7.2-source.zip
unzip antlr4-cpp-runtime-4.7.2-source.zip -d antlr4-cpp-runtime &&
rm antlr4-cpp-runtime-4.7.2-source.zip &&
pushd antlr4-cpp-runtime &&
mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local .. && make -j 4 && make install &&
popd &&
rm -rf antlr4-cpp-runtime'


# YAMLcpp
bash -c 'mkdir -p yaml-cpp-0.6.2 && \
git clone https://github.com/jbeder/yaml-cpp.git yaml-cpp && \
cd yaml-cpp && \
git checkout tags/yaml-cpp-0.6.2 && \
mkdir build && \
cd build && \
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local -DYAML_CPP_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS='-fPIC' .. && \
make -j 4 && \
make install && \
cd && \
rm -rf yaml-cpp'


# Celero
bash -c 'mkdir -p celero-v2.6.0 && \
git clone https://github.com/DigitalInBlue/Celero.git celero && \
cd celero && \
git checkout tags/v2.6.0 && \
mkdir build && \
cd build && \
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS=$CXX_FLAGS .. && \
make -j 4 && \
make install && \
cd && \
rm -rf celero'


# pip3 install cloudpickle
pip3 install cloudpickle
