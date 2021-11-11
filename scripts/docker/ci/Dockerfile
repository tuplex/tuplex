# docker image for cibuildhweel/github actions
# this is the x86_64 version
# build this image via  docker build -t tuplex/ci .
# and upload
FROM quay.io/pypa/manylinux2014_x86_64
MAINTAINER "Tuplex project@Brown"


# add script files
RUN mkdir -p /opt/sbin
ADD install_boost.sh /opt/sbin/install_boost.sh
ADD install_tuplex_reqs.sh /opt/sbin/install_tuplex_reqs.sh
ADD install_llvm9.sh /opt/sbin/install_llvm9.sh
# cmake not required to be installed, because recent image has cmake 3.20
# it uses gcc 9.3.1

# CentOS/RHEL does not use OpenSSL for the system curl, however AWSSDK must use OpenSSL backed curl.
ADD install_curl.sh /opt/sbin/install_curl.sh

# image is centos based, so use yum as package manager
# --> install_llvm9 uses most recent 9 release.
# yet, can also use yum?

# llvm9.0 can be then found in  /usr/lib64/llvm9.0/
# note that static libs areq required too!
#RUN yum install -y llvm9.0-devel llvm9.0-static wget

RUN yum update -y
RUN yum install -y wget

# install curl first
RUN bash /opt/sbin/install_curl.sh

# llvm-9 on yum repo might be broken, use manually built llvm
RUN bash /opt/sbin/install_llvm9.sh

# install boost-python for 3.7, 3.8, 3.9, 3.10
RUN bash /opt/sbin/install_boost.sh /opt/python/cp37-cp37m/bin/python3.7 /opt/boost/python3.7
RUN bash /opt/sbin/install_boost.sh /opt/python/cp38-cp38//bin/python3.8 /opt/boost/python3.8
RUN bash /opt/sbin/install_boost.sh /opt/python/cp39-cp39/bin/python3.9 /opt/boost/python3.9
RUN bash /opt/sbin/install_boost.sh /opt/python/cp310-cp310/bin/python3.10 /opt/boost/python3.10


# Update pip versions
RUN python3.7 -m pip install --upgrade pip setuptools wheel
RUN python3.8 -m pip install --upgrade pip setuptools wheel
RUN python3.9 -m pip install --upgrade pip setuptools wheel
RUN python3.10 -m pip install --upgrade pip setuptools wheel

# matrix?
RUN python3.7 -m pip install cloudpickle numpy pandas
RUN python3.8 -m pip install cloudpickle numpy pandas
RUN python3.9 -m pip install cloudpickle numpy pandas
# this an alpha release, pandas not yet supported...
RUN python3.10 -m pip install cloudpickle # numpy # pandas

# tuplex requirements
RUN bash /opt/sbin/install_tuplex_reqs.sh

# add lambda-specific Python 3.8 (full python install)
ADD install_lambda_python.sh /opt/sbin/install_lambda_python.sh
RUN bash /opt/sbin/install_lambda_python.sh

## MongoDB community edition for WebUI testing
ADD mongodb-org-5.0.repo /etc/yum.repos.d/mongodb-org-5.0.repo
RUN yum update -y && yum install -y mongodb-org


# remove all the tmp stuff
RUN rm -rf /tmp/*


# remove temp stuff based on https://www.getpagespeed.com/server-setup/clear-disk-space-centos
RUN curl -Ls http://bit.ly/clean-centos-disk-space | bash
