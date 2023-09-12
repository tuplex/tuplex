# docker image for cibuildhweel/github actions
# this is the x86_64 version
# build this image via  docker build -t tuplex/ci .
# and upload
# (c) 2017-2022 Tuplex team
FROM quay.io/pypa/manylinux2014_x86_64


MAINTAINER "Tuplex project@Brown"


# image is centos based, so use yum as package manager
# --> install_llvm uses most recent 9 release.

RUN yum update -y
RUN yum install -y dnf
RUN dnf install -y git flex curl autoconf zip wget kernel-headers python3

# add script files from local dir
RUN mkdir -p /opt/sbin

ADD install_cmake.sh /opt/sbin/install_cmake.sh
RUN bash /opt/sbin/install_cmake.sh amd64 linux 3.27.4 /usr/local

ADD install_boost.sh /opt/sbin/install_boost.sh
ADD install_tuplex_reqs.sh /opt/sbin/install_tuplex_reqs.sh
ADD install_llvm.sh /opt/sbin/install_llvm.sh

# cmake not required to be installed, because recent image has cmake 3.20
# it uses gcc 9.3.1

# CentOS/RHEL does not use OpenSSL for the system curl, however AWSSDK must use OpenSSL backed curl.
ADD install_curl.sh /opt/sbin/install_curl.sh



# install curl now
RUN bash /opt/sbin/install_curl.sh

# install boost-python for 3.7, 3.8, 3.9, 3.10
RUN bash /opt/sbin/install_boost.sh /opt/python/cp37-cp37m/bin/python3.7 /opt/boost/python3.7
RUN bash /opt/sbin/install_boost.sh /opt/python/cp38-cp38//bin/python3.8 /opt/boost/python3.8
RUN bash /opt/sbin/install_boost.sh /opt/python/cp39-cp39/bin/python3.9 /opt/boost/python3.9
RUN bash /opt/sbin/install_boost.sh /opt/python/cp310-cp310/bin/python3.10 /opt/boost/python3.10
RUN bash /opt/sbin/install_boost.sh /opt/python/cp311-cp311/bin/python3.11 /opt/boost/python3.11

# Update pip versions
RUN python3.7 -m pip install --upgrade pip setuptools wheel
RUN python3.8 -m pip install --upgrade pip setuptools wheel
RUN python3.9 -m pip install --upgrade pip setuptools wheel
RUN python3.10 -m pip install --upgrade pip setuptools wheel
RUN python3.11 -m pip install --upgrade pip setuptools wheel

# matrix?
RUN python3.7 -m pip install 'cloudpickle<2.0' cython numpy pandas
RUN python3.8 -m pip install 'cloudpickle<2.0' cython numpy pandas
RUN python3.9 -m pip install 'cloudpickle<2.0' numpy pandas
RUN python3.10 -m pip install 'cloudpickle>2.0' numpy pandas
RUN python3.11 -m pip install 'cloudpickle>2.0' numpy pandas

# tuplex requirements
RUN bash /opt/sbin/install_tuplex_reqs.sh

#RUN bash /opt/sbin/install_boost.sh /opt/python/cp310-cp310/bin/python3.10 /opt/boost/python3.10

# add lambda-specific Python 3.8 (full python install)
ADD install_lambda_python.sh /opt/sbin/install_lambda_python.sh
RUN bash /opt/sbin/install_lambda_python.sh

## MongoDB community edition for WebUI testing
ADD mongodb-org-5.0.repo /etc/yum.repos.d/mongodb-org-5.0.repo
RUN yum update -y && yum install -y mongodb-org

# replace curl again with recent version to be 100% everything worked properly.
RUN bash /opt/sbin/install_curl.sh

# remove all the tmp stuff
RUN rm -rf /tmp/*

# remove temp stuff based on https://www.getpagespeed.com/server-setup/clear-disk-space-centos
RUN curl -Ls http://bit.ly/clean-centos-disk-space | bash

# install additional libraries for debugging
RUN yum install -y centos-release-scl-rh devtoolset-11-libtsan-devel devtoolset-10-libtsan-devel
RUN yum install -y devtoolset-11-libasan-devel devtoolset-10-libasan-devel
