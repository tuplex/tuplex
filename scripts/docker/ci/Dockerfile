# docker image for cibuildhweel/github actions
# this is the x86_64 version
# build this image via  docker build -t tuplex/ci .
# and upload
# (c) 2017-2022 Tuplex team
FROM quay.io/pypa/manylinux2014_x86_64

MAINTAINER "Tuplex project@Brown"

# select core versions to use when building CI image here
ARG PYTHON_VERSION=3.11.5
ARG CMAKE_VERSION=3.27.5
ARG BOOST_VERSION=1.79.0

# image is centos based, so use yum as package manager
# --> install_llvm uses most recent 16 release.

# set link to desired python version, note that ${PYTHON_VERSION%.*} gives for e.g. 3.x.y -> 3.x
RUN ln -sf /usr/local/bin/python${PYTHON_VERSION%.*} /usr/local/bin/python3 && /usr/local/bin/python3 --version

RUN yum update -y && yum install -y dnf && dnf install -y git autoconf zip wget

# add script files from local dir
RUN mkdir -p /opt/sbin

ADD install_cmake.sh /opt/sbin/install_cmake.sh
RUN bash /opt/sbin/install_cmake.sh amd64 linux ${CMAKE_VERSION} /usr/local

ADD install_boost.sh /opt/sbin/install_boost.sh

ADD install_llvm.sh /opt/sbin/install_llvm.sh

# CentOS/RHEL does not use OpenSSL for the system curl, however AWSSDK must use OpenSSL backed curl.
ADD install_curl.sh /opt/sbin/install_curl.sh


# install curl now
RUN bash /opt/sbin/install_curl.sh

# install boost-python for given version
RUN bash /opt/sbin/install_boost.sh /usr/local/bin/python3 /opt/boost/python${PYTHON_VERSION%.*} ${BOOST_VERSION}

# Update pip versions
RUN /usr/local/bin/python3 -m pip install --upgrade pip setuptools wheel

# Tuplex requirements, installs python version specific requirements as well
ADD install_tuplex_reqs.sh /opt/sbin/install_tuplex_reqs.sh
RUN bash /opt/sbin/install_tuplex_reqs.sh

# add lambda-specific Python (full python install), for correct shipping Python must be built with correct flags
ADD install_lambda_python.sh /opt/sbin/install_lambda_python.sh
RUN bash /opt/sbin/install_lambda_python.sh ${PYTHON_VERSION}

# install llvm, use here script which uses 16.0.6 to enable recent CPU architectures
RUN bash /opt/sbin/install_llvm.sh

## MongoDB community edition for WebUI testing
ADD mongodb-org-5.0.repo /etc/yum.repos.d/mongodb-org-5.0.repo
RUN yum update -y && yum install -y mongodb-org

# replace curl again with recent version to be 100% everything worked properly.
RUN bash /opt/sbin/install_curl.sh

# remove all the tmp stuff
# remove temp stuff based on https://www.getpagespeed.com/server-setup/clear-disk-space-centos
RUN rm -rf /tmp/* && curl -Ls http://bit.ly/clean-centos-disk-space | bash && dnf clean all && rm -rf /var/cache/yum

# uncomment following to install optional packages for debugging
# RUN yum install -y devtoolset-10-gdb
# RUN yum install -y centos-release-scl-rh devtoolset-11-libtsan-devel devtoolset-10-libtsan-devel
# RUN yum install -y devtoolset-11-libasan-devel devtoolset-10-libasan-devel
