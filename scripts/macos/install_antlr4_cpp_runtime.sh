#!/usr/bin/env bash
# this is a script to install the antlr4 runtime

# specify here target location
PREFIX=/usr/local

git clone https://github.com/antlr/antlr4.git \
&& cd antlr4 && cd runtime &&  git fetch --all --tags \
&& git checkout tags/4.9.3 -b 4.9.3 && cd Cpp/ && ./deploy-macos.sh \
&& unzip antlr4-cpp-runtime-macos.zip \
&& cd lib && mv * $PREFIX/lib/ && cd .. \
&& mv antlr4-runtime $PREFIX/include/ \
&& echo "ANTLR4 Cpp runtime installed to $PREFIX"
