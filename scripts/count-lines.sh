#!/usr/bin/env bash

# count all source files comprising tuplex
pushd ../tuplex
cloc core codegen/grammar codegen/include codegen/src adapters io python runtime utils awslambda
popd
