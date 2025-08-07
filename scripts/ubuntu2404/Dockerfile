# Tuplex ubuntu 24.04 image, holds code in /code directory and allows to build tuplex easily.
# create with docker build -t tuplex/ubuntu:24.04 .
# Run with docker run -it tuplex/ubuntu:24.04 bash
FROM ubuntu:24.04
LABEL authors="leonhards"

# Versions, environment variables.
# Needs to be a pyenv supported version: pyenv install --list.
ENV PYTHON_VERSION=3.13.5
ENV GITHUB_BRANCH=master

WORKDIR /tmp

# Install python version
# set the variables as per $(pyenv init -)
ENV LANG="C.UTF-8" \
    LC_ALL="C.UTF-8" \
    PATH="/opt/pyenv/shims:/opt/pyenv/bin:$PATH" \
    PYENV_ROOT="/opt/pyenv" \
    PYENV_SHELL="bash"

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        git \
        libbz2-dev \
        libffi-dev \
        libncurses5-dev \
        libncursesw5-dev \
        libreadline-dev \
        libsqlite3-dev \
        liblzma-dev \
        libssl-dev \
        make \
        netbase \
        pkg-config \
        tk-dev \
        wget \
        xz-utils \
        zlib1g-dev \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


RUN git clone --single-branch --depth 1 https://github.com/pyenv/pyenv.git $PYENV_ROOT \
    && pyenv install $PYTHON_VERSION \
    && pyenv global $PYTHON_VERSION \
    && find $PYENV_ROOT/versions -type d '(' -name '__pycache__' -o -name 'test' -o -name 'tests' ')' -exec rm -rf '{}' + \
    && find $PYENV_ROOT/versions -type f '(' -name '*.pyo' -o -name '*.exe' ')' -exec rm -f '{}' + \
    && rm -rf /tmp/*


# --- install requirements ---

ADD install_requirements.sh /tmp/install_requirements.sh

RUN /tmp/install_requirements.sh

# installs tuplex-specific dependencies into /opt
RUN mkdir -p /opt

# downloads/clones tuplex into /code directory
RUN mkdir -p /code

RUN rm -rf /tmp/*

WORKDIR /code

# Clone tuplex, and build initial version as well as install in python.
RUN git clone -b ${GITHUB_BRANCH} --single-branch https://github.com/tuplex/tuplex.git

# Install python dependencies
RUN python3 -m pip install cloudpickle numpy pandas

#