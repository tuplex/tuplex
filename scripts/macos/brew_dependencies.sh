#!/usr/bin/env bash
# This script installs all required dependencies via brew
# for instructions on how to install brew, visit https://brew.sh/

brew install coreutils protobuf zstd zlib libmagic llvm@9 pcre2 gflags yaml-cpp celero wget boost googletest

# latest antlr4-cpp-runtime 4.10 and googletest have a conflict
# in addition to 4.10 requiring C++20 to compile.
# Therefore, install old 4.9.3 Antlr4 version
# i.e., it used to be brew install antlr4-cpp-runtime, now use the following:
#brew tap-new tuplex/brew
#brew extract --version='4.9.3' antlr4-cpp-runtime tuplex/brew
#brew install antlr4-cpp-runtime@4.9.3
# brew install antlr4-cpp-runtime
