# Tuplex: Blazing Fast Python Data Science

[![Build Status](https://dev.azure.com/leonhardspiegelberg/Tuplex%20-%20Open%20Source/_apis/build/status/tuplex.tuplex?branchName=master)](https://dev.azure.com/leonhardspiegelberg/Tuplex%20-%20Open%20Source/_build/latest?definitionId=2&branchName=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![Supported python versions](https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9-blue)
[![Gitter](https://badges.gitter.im/tuplex/community.svg)](https://gitter.im/tuplex/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![PyPi Downloads](https://img.shields.io/pypi/dm/tuplex)](https://img.shields.io/pypi/dm/tuplex)

[Website](https://tuplex.cs.brown.edu/) [Documentation](https://tuplex.cs.brown.edu/python-api.html)

Tuplex is a parallel big data processing framework that runs data science pipelines written in Python at the speed of compiled code. 
Tuplex has similar Python APIs to [Apache Spark](https://spark.apache.org/) or [Dask](https://dask.org/), but rather than invoking the Python interpreter, Tuplex generates optimized LLVM bytecode for the given pipeline and input data set. Under the hood, Tuplex is based on data-driven compilation and dual-mode processing, two key techniques that make it possible for Tuplex to provide speed comparable to a pipeline written in hand-optimized C++.

You can join the discussion on Tuplex on our [Gitter community](https://gitter.im/tuplex/community) or read up more on the background of Tuplex in our [SIGMOD'21 paper](https://dl.acm.org/doi/abs/10.1145/3448016.3457244).

Contributions welcome!


### Contents
+ [Installation](#installation)
    - [Docker image](#docker)
    - [Pypi](#pypi)
+ [Building](#building)
    - [MacOS build from source](#macos-build-from-source)
    - [Ubuntu build from source](#ubuntu-build-from-source)
    - [Customizing the build](#customizing-the-build)
+ [Example](#example)
+ [License](#license)

### Installation
To install Tuplex, you can use a PyPi package for Linux, or a Docker container for MacOS which will launch a jupyter notebook with Tuplex preinstalled.
#### Docker
```
docker run -p 8888:8888 tuplex/tuplex
```
#### PyPI
```
pip install tuplex
```

### Building

Tuplex is available for MacOS and Linux. The current version has been tested under MacOS 10.13-10.15 and Ubuntu 18.04 and 20.04 LTS.
To install Tuplex, simply install the dependencies first and then build the package.

#### MacOS build from source
To build Tuplex, you need several other packages first which can be easily installed via [brew](https://brew.sh/).
```
brew install llvm@9 boost boost-python3 aws-sdk-cpp pcre2 antlr4-cpp-runtime googletest gflags yaml-cpp celero protobuf libmagic
python3 -m pip cloudpickle numpy
python3 setup.py install
```

#### Ubuntu build from source
To faciliate installing the dependencies for Ubuntu, we do provide two scripts (`scripts/ubuntu1804/install_reqs.sh` for Ubuntu 18.04, or `scripts/ubuntu2004/install_reqs.sh` for Ubuntu 20.04). To create an up to date version of Tuplex, simply run
```
./scripts/ubuntu1804/install_reqs.sh
python3 -m pip cloudpickle numpy
python3 setup.py install
```

#### Customizing the build

Besides building a pip package, cmake can be also directly invoked. To compile the package via cmake
```
mkdir build
cd build
cmake ..
make -j$(nproc)
```
The python package corresponding to Tuplex can be then found in `build/dist/python` with C++ test executables based on googletest in `build/dist/bin`.

To customize the cmake build, the following options are available to be passed via `-D<option>=<value>`:

| option | values | description |
| ------ | ------ | ----------- |
| `CMAKE_BUILD_TYPE` | `Release` (default), `Debug`, `RelWithDebInfo`, `tsan`, `asan`, `ubsan` | select compile mode. Tsan/Asan/Ubsan correspond to Google Sanitizers. |
| `BUILD_WITH_AWS` | `ON` (default), `OFF` | build with AWS SDK or not. On Ubuntu this will build the Lambda executor. |
| `BUILD_WITH_ORC` | `ON`, `OFF` (default) | build with ORC file format support. |
| `SKIP_AWS_TESTS` | `ON` (default), `OFF` | skip aws tests, helpful when no AWS credentials/AWS Tuplex chain is setup. |
| `GENERATE_PDFS` | `ON`, `OFF` (default) | output in Debug mode PDF files if graphviz is installed (e.g., `brew install graphviz`) for ASTs of UDFs, query plans, ...|
| `PYTHON3_VERSION` | `3.6`, ... | when trying to select a python3 version to build against, use this by specifying `major.minor`. To specify the python executable, use the options provided by [cmake](https://cmake.org/cmake/help/git-stage/module/FindPython3.html). |
| `LLVM_ROOT_DIR` | e.g. `/usr/lib/llvm-9` | specify which LLVM version to use |
| `BOOST_DIR` | e.g. `/opt/boost` | specify which Boost version to use. Note that the python component of boost has to be built against the python version used to build Tuplex |

For example, to create a debug build which outputs PDFs use the following snippet:

```
cmake -DCMAKE_BUILD_TYPE=Debug -DGENERATE_PDFS=ON ..
```

### Example
Tuplex can be used in python interactive mode, a jupyter notebook or by copying the below code to a file. To try it out, run the following example:

```python
from tuplex import *
c = Context()
res = c.parallelize([1, 2, None, 4]).map(lambda x: (x, x * x)).collect()
# this prints [(1, 1), (2, 4), (4, 16)]
print(res)
```

More examples can be found [here](https://tuplex.cs.brown.edu/gettingstarted.html).

### License
Tuplex is available under Apache 2.0 License, to cite the paper use:

```bibtex
@inproceedings{10.1145/3448016.3457244,
author = {Spiegelberg, Leonhard and Yesantharao, Rahul and Schwarzkopf, Malte and Kraska, Tim},
title = {Tuplex: Data Science in Python at Native Code Speed},
year = {2021},
isbn = {9781450383431},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3448016.3457244},
doi = {10.1145/3448016.3457244},
booktitle = {Proceedings of the 2021 International Conference on Management of Data},
pages = {1718–1731},
numpages = {14},
location = {Virtual Event, China},
series = {SIGMOD/PODS '21}
}
```

---
(c) 2017-2021 Tuplex contributors
