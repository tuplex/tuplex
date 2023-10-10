#!/usr/bin/env bash
# this is a script to install the antlr4 runtime

# specify here target location
PREFIX=/usr/local

# if antlr4 exists already, skip
[ -d "antlr4" ] && exit 0

git clone https://github.com/antlr/antlr4.git \
&& cd antlr4 && cd runtime &&  git fetch --all --tags \
&& git checkout tags/4.13.1 -b 4.13.1 && cd Cpp/ && ./deploy-macos.sh \
&& unzip -l antlr4-cpp-runtime-macos.zip && unzip antlr4-cpp-runtime-macos.zip \
&& cd lib && cp -R * $PREFIX/lib/ && cd .. \
&& mv antlr4-runtime $PREFIX/include/ \
&& echo "ANTLR4 Cpp runtime installed to $PREFIX"

# execute copy command (fix for delocate wheel)
ls -l /usr/local/include
ls -l /usr/local/lib

cp lib/libantlr4-runtime.dylib /Users/runner/work/tuplex/tuplex/libantlr4-runtime.dylib

exit 0
