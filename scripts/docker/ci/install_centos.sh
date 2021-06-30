#!/usr/bin/env bash
# centos (manylinux) install script
cd ./ci

# create opt dir
mkdir -p /opt
mkdir -p /opt/lib
mkdir -p /opt/bin
mkdir -p /opt/include

# add script files
bash install_llvm9.sh
bash install_python_packages.sh
bash install_tuplex_reqs.sh

# cmake not required to be installed, because recent image has cmake 3.20
# it uses gcc 9.3.1


# image is centos based, so use yum as package manager
# --> install_llvm9 uses most recent 9 release.
# yet, can also use yum?

# llvm9.0 can be then found in  /usr/lib64/llvm9.0/
RUN yum install -y llvm9.0-devel

# install boost-python for 3.7, 3.8, 3.9, 3.10
bash install_boost.sh /opt/python/cp37-cp37m/bin/python3.7 /opt/boost/python3.7
bash install_boost.sh /opt/python/cp38-cp38//bin/python3.8 /opt/boost/python3.8
bash install_boost.sh /opt/python/cp39-cp39/bin/python3.9 /opt/boost/python3.9
bash install_boost.sh /opt/python/cp310-cp310/bin/python3.10 /opt/boost/python3.10

# matrix?
python3.7 -m pip install cloudpickle numpy
python3.8 -m pip install cloudpickle numpy
python3.9 -m pip install cloudpickle numpy
python3.10 -m pip install cloudpickle numpy

# tuplex requirements
bash install_tuplex_reqs.sh