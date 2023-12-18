# Docker image based on Ubuntu 20.04 which holds all frameworks for evaluation
# (c) 2017 - 2023 L.Spiegelberg, R. Yesantharao

FROM ubuntu:20.04
MAINTAINER Leonhard Spiegelberg "leonhard_spiegelberg@brown.edu"

# Use /tmp folder to build everything...
ENV DEBIAN_FRONTEND=noninteractive

# (1) general packages
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    apt-utils vim build-essential libssl-dev zlib1g-dev libncurses5-dev \
    libncursesw5-dev libreadline-dev libsqlite3-dev libgdbm-dev libdb5.3-dev \
     libbz2-dev libexpat1-dev liblzma-dev tk-dev libffi-dev wget git curl libcurl4-openssl-dev

RUN /bin/bash -c "mkdir /opt; mkdir /code; mkdir /data; mkdir /results"

# (2.1) install python 3.6.12 system-wide because that's the stable version for pypy (v7.3.3)
# cf. https://doc.pypy.org/en/latest/release-v7.3.3.html
#RUN /bin/bash -c "cd /tmp && wget https://www.python.org/ftp/python/3.6.12/Python-3.6.12.tgz && tar xf Python-3.6.12.tgz \
#    && cd Python-3.6.12 && ./configure --prefix=/opt --enable-optimizations && make -j $(nproc) \
#    && make altinstall"
#COPY install_python3.sh /tmp/install_python3.sh
#RUN /bin/bash /tmp/install_python3.sh


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

ENV PYTHON_VERSION 3.6.9

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


# TODO: LS --> Pypy 7.3.3.

# install cmake 3.19.7
COPY install_cmake.sh /usr/src/install_cmake.sh
RUN /bin/bash /usr/src/install_cmake.sh

## TODO: LS --> Weld
# install llvm6.0 (required for weld)
COPY install_llvm6.sh /usr/src/install_llvm6.sh
RUN /bin/bash /usr/src/install_llvm6.sh

# install llvm9.0 (required for Tuplex)
COPY install_llvm9.sh /usr/src/install_llvm9.sh
RUN /bin/bash /usr/src/install_llvm9.sh

# TODO: LS --> Spark 2.4.7 + Scala 2.12.10
COPY install_spark.sh /usr/src/install_spark.sh
RUN /bin/bash /usr/src/install_spark.sh

# install pypy3
COPY install_pypy3.sh /usr/src/install_pypy3.sh
RUN /bin/bash /usr/src/install_pypy3.sh

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

# install weld (last)
ADD PatchedGrizzlyMakefile.make /usr/src/PatchedGrizzlyMakefile.make
COPY install_weld.sh /usr/src/install_weld.sh
RUN /bin/bash /usr/src/install_weld.sh

# clean
RUN rm -rf /usr/src
RUN rm -rf /tmp/*

#TODO: pypy...
