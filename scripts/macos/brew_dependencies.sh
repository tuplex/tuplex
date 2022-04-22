#!/usr/bin/env bash
# install dependencies via brew

brew install coreutils protobuf zstd zlib libmagic llvm@9 pcre2 gflags yaml-cpp celero wget boost
brew antlr4-cpp-runtime
brew googletest

# antlr4-cpp-runtime and googletest have a conflict
# force googletest
 brew link --overwrite googletest
 echo "done!"
