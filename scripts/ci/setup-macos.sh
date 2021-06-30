#!/usr/bin/env bash
# use brew to setup everything

set -ex

# this should setup python3.9
brew install python3
brew upgrade python3
brew link --force --overwrite python3

brew install llvm@9 boost boost-python3 aws-sdk-cpp pcre2 antlr4-cpp-runtime googletest gflags yaml-cpp celero

python3.9 -m pip install cloudpickle numpy