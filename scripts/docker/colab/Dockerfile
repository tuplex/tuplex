# Docker image mimicking Google Colab setup for testing purposes
FROM ubuntu:18.04

# Fix timezone to US
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Python version used is 3.7.12
RUN apt-get update && apt-get install -y wget curl build-essential zlib1g-dev libssl1.0-dev libncurses-dev libgdbm-dev libz-dev tk-dev libsqlite3-dev libreadline-dev liblzma-dev libffi-dev

WORKDIR /usr/src
RUN cd /usr/src && curl https://www.openssl.org/source/openssl-1.0.2o.tar.gz | tar xz && cd openssl-1.0.2o && ./config shared zlib --prefix=/usr/local/ && make && make install -j8

RUN wget https://www.python.org/ftp/python/3.7.12/Python-3.7.12.tgz

# DO not use enable-optimizations because it gets stuck...
RUN tar xf Python-3.7.12.tgz && cd Python-3.7.12 && ./configure --prefix=/usr --with-openssl=/usr/local --with-ensurepip=install &&  make install -j8

WORKDIR /work
ADD requirements.txt /work/requirements.txt
