#!/usr/bin/env bash

export CFLAGS=-I/usr/include/openssl

# from https://bugs.python.org/issue36044
# change tasks, because hangs at test_faulthandler...
export PROFILE_TASK=-m test.regrtest --pgo \
        test_collections \
        test_dataclasses \
        test_difflib \
        test_embed \
        test_float \
        test_functools \
        test_generators \
        test_int \
        test_itertools \
        test_json \
        test_logging \
        test_long \
        test_ordered_dict \
        test_pickle \
        test_pprint \
        test_re \
        test_set \
        test_statistics \
        test_struct \
        test_tabnanny \
        test_xml_etree

set -ex && cd /tmp && wget https://www.python.org/ftp/python/3.6.9/Python-3.6.9.tgz && tar xf Python-3.6.9.tgz \
    && cd Python-3.6.9 && ./configure --with-lto --prefix=/opt --enable-optimizations \
    && make -j $(( 1 * $( egrep '^processor[[:space:]]+:' /proc/cpuinfo | wc -l ) )) \
    && make altinstall