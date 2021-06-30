#!/usr/bin/env bash

cd /usr/local/include/ && ln -s python3.6m python3.6 && cd - || exit

cd /usr/src || exit
wget https://dl.bintray.com/boostorg/release/1.75.0/source/boost_1_75_0.tar.gz
tar xf boost_1_75_0.tar.gz
cd /usr/src/boost_1_75_0 || exit

./bootstrap.sh --with-python=python3.6 --prefix=/opt --with-libraries="thread,iostreams,regex,system,filesystem,python,stacktrace,atomic,chrono,date_time"
./b2 cxxflags="-fPIC" link=static -j "$(nproc)"
./b2 cxxflags="-fPIC" link=static install