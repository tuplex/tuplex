#!/usr/bin/env bash

# detect platform


# build wheel on mac os
CMAKE_ARGS="-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON -DBoost_USE_STATIC_LIBS=ON -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON" python3 setup.py bdist_wheel
