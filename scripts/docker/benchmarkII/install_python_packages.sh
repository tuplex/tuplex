#!/usr/bin/env bash

# Note: tableauhyperapi needs pip > 19.3
# install latest pip therefore first
pip3 install --upgrade pip

# install python packages for tuplex (needs cloudpickle to compile, numpy to run certain tests)
pip3 install 'cloudpickle<2.0.0' numpy==1.15.4
# install python compilers to benchmark against
pip3 install Nuitka==0.6.13 Cython==0.29.22
# install Pandas + Dask to benchmark against
pip3 install pandas==1.1.5 "dask[complete]"==2021.03
# install Hyper to benchmark against
pip3 install tableauhyperapi==0.0.12366

# if pypy3 exists, also install packages!
if ! [ -x "$(command -v pypy3 --version)" ]; then
   export PATH=/opt/pypy3/bin/:$PATH
fi
if [ -x "$(command -v pypy3 --version)" ]; then
  pypy3 -m ensurepip
  pypy3 -m pip install numpy==1.15.4 pandas==1.1.5 "dask[complete]"==2021.03
fi
