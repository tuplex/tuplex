# Docker image based on Ubuntu 20.04 which holds all frameworks for evaluation
# (c) 2021 L.Spiegelberg, R. Yesantharao

FROM ubuntu:20.04
MAINTAINER Leonhard Spiegelberg "leonhard_spiegelberg@brown.edu"

# Use /tmp folder to build everything...
ENV DEBIAN_FRONTEND=noninteractive

# (1) general packages
RUN mkdir -p /opt/sbin
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    apt-utils vim build-essential libssl-dev zlib1g-dev libncurses5-dev \
    libncursesw5-dev libreadline-dev libsqlite3-dev libgdbm-dev libdb5.3-dev \
     libbz2-dev libexpat1-dev liblzma-dev tk-dev libffi-dev wget git
# other packages
RUN apt-get install -y build-essential software-properties-common wget libedit-dev libz-dev python3-yaml python3-pip pkg-config libssl-dev libcurl4-openssl-dev curl uuid-dev git libffi-dev libmagic-dev doxygen doxygen-doc doxygen-latex doxygen-gui graphviz libgflags-dev libncurses-dev awscli openjdk-8-jdk libyaml-dev ninja-build gcc-10 g++-10 autoconf libtool m4

# CentOS/RHEL does not use OpenSSL for the system curl, however AWSSDK must use OpenSSL backed curl.
ADD install_curl.sh /opt/sbin/install_curl.sh
RUN bash /opt/sbin/install_curl.sh

# create dirs
RUN /bin/bash -c "mkdir /opt; mkdir /code; mkdir /data; mkdir /results"

# from https://github.com/infosiftr/python/blob/f82205cde8f0a5ffa276103a50d843edced67757/3.6/alpine3.10/Dockerfile

# ensure local python is preferred over distribution python
ENV PATH /usr/local/bin:$PATH

# http://bugs.python.org/issue19846
# > At the moment, setting "LANG=C" on a Linux system *fundamentally breaks Python 3*, and that's not OK.
ENV LANG C.UTF-8

# runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
		ca-certificates \
		netbase \
	&& rm -rf /var/lib/apt/lists/*

ENV PYTHON_VERSION 3.9.8

RUN set -ex \
	\
	&& savedAptMark="$(apt-mark showmanual)" \
	&& apt-get update && apt-get install -y --no-install-recommends \
		dpkg-dev \
		gcc \
		libbz2-dev \
		libc6-dev \
		libexpat1-dev \
		libffi-dev \
		libgdbm-dev \
		liblzma-dev \
		libncursesw5-dev \
		libreadline-dev \
		libsqlite3-dev \
		libssl-dev \
		make \
		tk-dev \
		wget \
		xz-utils \
		zlib1g-dev \
	&& wget -O python.tar.xz "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz" \
	&& wget -O python.tar.xz.asc "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz.asc" \
	&& export GNUPGHOME="$(mktemp -d)" \
	&& rm -rf "$GNUPGHOME" python.tar.xz.asc \
	&& mkdir -p /usr/src/python \
	&& tar -xJC /usr/src/python --strip-components=1 -f python.tar.xz \
	&& rm python.tar.xz \
	\
	&& cd /usr/src/python \
	&& gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)" \
	&& ./configure \
		--build="$gnuArch" \
		--enable-loadable-sqlite-extensions \
		--enable-optimizations \
		--enable-shared \
		--with-system-expat \
		--with-system-ffi \
		--without-ensurepip \
	&& make -j "$(nproc)" \
# https://github.com/docker-library/python/issues/160#issuecomment-509426916
		PROFILE_TASK='-m test.regrtest --pgo \
			test_array \
			test_base64 \
			test_binascii \
			test_binhex \
			test_binop \
			test_bytes \
			test_c_locale_coercion \
			test_class \
			test_cmath \
			test_codecs \
			test_compile \
			test_complex \
			test_csv \
			test_decimal \
			test_dict \
			test_float \
			test_fstring \
			test_hashlib \
			test_io \
			test_iter \
			test_json \
			test_long \
			test_math \
			test_memoryview \
			test_pickle \
			test_re \
			test_set \
			test_slice \
			test_struct \
			test_threading \
			test_time \
			test_traceback \
			test_unicode \
		' \
	&& make install \
	&& ldconfig \
	\
	&& apt-mark auto '.*' > /dev/null \
	&& apt-mark manual $savedAptMark \
	&& find /usr/local -type f -executable -not \( -name '*tkinter*' \) -exec ldd '{}' ';' \
		| awk '/=>/ { print $(NF-1) }' \
		| sort -u \
		| xargs -r dpkg-query --search \
		| cut -d: -f1 \
		| sort -u \
		| xargs -r apt-mark manual \
	&& apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
	&& rm -rf /var/lib/apt/lists/* \
	\
	&& find /usr/local -depth \
		\( \
			\( -type d -a \( -name test -o -name tests \) \) \
			-o \
			\( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
		\) -exec rm -rf '{}' + \
	&& rm -rf /usr/src/python \
	\
	&& python3 --version

# make some useful symlinks that are expected to exist
RUN cd /usr/local/bin \
	&& ln -s idle3 idle \
	&& ln -s pydoc3 pydoc \
	&& ln -s python3 python \
	&& ln -s python3-config python-config

# if this is called "PIP_VERSION", pip explodes with "ValueError: invalid truth value '<VERSION>'"
ENV PYTHON_PIP_VERSION 19.1.1

RUN set -ex; \
	\
	savedAptMark="$(apt-mark showmanual)"; \
	apt-get update; \
	apt-get install -y --no-install-recommends wget; \
	\
	wget -O get-pip.py 'https://bootstrap.pypa.io/pip/3.6/get-pip.py'; \
	\
	apt-mark auto '.*' > /dev/null; \
	[ -z "$savedAptMark" ] || apt-mark manual $savedAptMark; \
	apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
	rm -rf /var/lib/apt/lists/*; \
	\
	python get-pip.py \
		--disable-pip-version-check \
		--no-cache-dir \
		"pip==$PYTHON_PIP_VERSION" \
	; \
	pip --version; \
	\
	find /usr/local -depth \
		\( \
			\( -type d -a \( -name test -o -name tests \) \) \
			-o \
			\( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
		\) -exec rm -rf '{}' +; \
	rm -f get-pip.py

# install recent cmake
COPY install_cmake.sh /usr/src/install_cmake.sh
RUN /bin/bash /usr/src/install_cmake.sh

# install llvm9.0 (required for Tuplex)
COPY install_llvm9.sh /usr/src/install_llvm9.sh
RUN /bin/bash /usr/src/install_llvm9.sh

# TODO: LS --> Spark 2.4.7 + Scala 2.12.10
COPY install_spark.sh /usr/src/install_spark.sh
RUN /bin/bash /usr/src/install_spark.sh

# install all python (& pypy) packages (i.e. pandas/dask/...)
COPY install_python_packages.sh /usr/src/install_python_packages.sh
RUN /bin/bash /usr/src/install_python_packages.sh

# install recent boost
ADD install_boost.sh /usr/src/install_boost.sh
RUN /bin/bash /usr/src/install_boost.sh

## (2.2) add install reqs script & execute
ADD install_tuplex_reqs.sh /usr/src/install_tuplex_reqs.sh
RUN /bin/bash /usr/src/install_tuplex_reqs.sh

# install sbt
ADD install_sbt.sh /usr/src/install_sbt.sh
RUN /bin/bash /usr/src/install_sbt.sh

# install lambda-python
ADD install_lambda_python.sh /usr/src/install_lambda_python.sh
RUN /bin/bash /usr/src/install_lambda_python.sh

# remove the faulty cloudpickle 2.1.0 installation
RUN python3.9 -m pip uninstall cloudpickle -y && python3.9 -m pip install 'cloudpickle<2.0.0'

# clean
RUN rm -rf /usr/src
RUN rm -rf /tmp/*
