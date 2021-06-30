#!/usr/bin/env bash
# installs pypy 7.3.3 which is compatible with python 3.6
# there is also python3.7 version, but it is in beta...
cd /usr/src && wget https://downloads.python.org/pypy/pypy3.6-v7.3.3-linux64.tar.bz2 \
&& tar xf pypy3.6-v7.3.3-linux64.tar.bz2 && mkdir /opt/pypy3 \
&& mv pypy3.6-v7.3.3-linux64/* /opt/pypy3/