#!/usr/bin/env bash

# Note: tableauhyperapi needs pip > 19.3
# install latest pip therefore first
pip3 install --upgrade pip

# install python packages for tuplex (needs cloudpickle to compile, numpy to run certain tests)
pip3 install cloudpickle numpy
# install python compilers to benchmark against
pip3 install Nuitka Cython
# install Pandas + Dask to benchmark against
pip3 install pandas "dask[complete]"
# install Hyper to benchmark against
pip3 install tableauhyperapi

# if pypy3 exists, also install packages!
if ! [ -x "$(command -v pypy3 --version)" ]; then
   export PATH=/opt/pypy3/bin/:$PATH
fi
if [ -x "$(command -v pypy3 --version)" ]; then
  pypy3 -m ensurepip
  pypy3 -m pip install numpy pandas "dask[complete]"
fi
