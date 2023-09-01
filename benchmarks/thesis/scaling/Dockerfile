# this image has python 3.1-3.9, use pyenv to add a couple more versions
FROM fkrull/multi-python:bionic

RUN apt-get update
RUN apt-get install -y --no-install-recommends make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

ENV HOME="/root"
WORKDIR ${HOME}
RUN apt-get install -y git
RUN git clone --depth=1 https://github.com/pyenv/pyenv.git .pyenv
ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"

ENV PYTHON_VERSION=3.8.6
RUN pyenv install ${PYTHON_VERSION}
RUN pyenv global ${PYTHON_VERSION}


RUN mkdir -p .venv
RUN eval "$(${PYENV_ROOT}/bin/pyenv init -)" && ${PYENV_ROOT}/bin/pyenv local ${PYTHON_VERSION}

## install various python versions (latest in each gen)

## python <= 3.5 require OpenSSL 1.0
#RUN mkdir -p /opt/openssl-1.0
#RUN cd /tmp && wget http://www.openssl.org/source/openssl-1.0.0a.tar.gz && tar -xf openssl-1.0.0a.tar.gz && tar -xf openssl-1.0.0a.tar.gz && cd openssl-1.0.0a && ./config --prefix=/opt/openssl-1.0 && make && make install_sw

# others OpenSSL 1.1

# hence, install here backup version of OpenSSL 1.0
# i.e., cf. https://github.com/pyenv/pyenv/wiki/Common-build-problems
#RUN CPPFLAGS="-I/opt/openssl-1.0/include/" LDFLAGS="-L/opt/openssl-1.0/lib/" pyenv install 3.1.5
#RUN pyenv install 3.2.6
#RUN pyenv install 3.3.7
#RUN pyenv install 3.4.10
#RUN pyenv install 3.5.10
#RUN pyenv install 3.6.15
#RUN pyenv install 3.7.16
#RUN pyenv install 3.8.16
#RUN pyenv install 3.9.16

# add missing versions via pyenv
# can then invoke benchmarking via python3.1 ... or pyenv local 3.10.11 && python3.10 ...
RUN pyenv install 3.10.11
RUN pyenv install 3.11.3
RUN pyenv install 3.12.0a7

# fix argparse for python3.1
RUN cd /tmp && wget https://www.piwheels.org/simple/setuptools/setuptools-0.9.8-py2.py3-none-any.whl#sha256=bf8e39b012d0146a4aeaf541353f5b9b9630bbcb5ac27505849ac896bcff2f6a
RUN cd /tmp && git clone https://github.com/ThomasWaldmann/argparse.git && cd argparse && PYTHONPATH="/usr/lib/python3.1/dist-packages/"

# add script files & data to run benchmarks
RUN mkdir -p /code/scripts
ADD ./scripts/countprimes.py /code/scripts/countprimes.py
ADD ./scripts/z2_query.py /code/scripts/z2_query.py
RUN mkdir -p /data/zillow
ADD .cache/large100MB.csv /data/zillow/large100MB.csv
ADD .cache/large1GB.csv /data/zillow/large1GB.csv

ADD ./scripts/benchmark.sh /code/benchmark.sh