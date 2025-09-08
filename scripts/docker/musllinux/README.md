# Tuplex Dependencies Installation Scripts

This directory contains modular installation scripts for building Tuplex on Alpine Linux. Each script installs a specific dependency, allowing for flexible and optimized Docker builds.

## Scripts Overview

### Base System Dependencies
- **`install_alpine.sh`** - Installs Alpine Linux packages and CMake
- **`install_zlib.sh`** - Installs zlib-ng compression library

### Core Compiler Infrastructure
- **`install_llvm.sh`** - Installs LLVM and Clang compiler
- **`install_boost.sh`** - Installs Boost C++ libraries

### Testing and Benchmarking
- **`install_googletest.sh`** - Installs Google Test framework
- **`install_celero.sh`** - Installs Celero benchmarking library

### Data Processing Libraries
- **`install_snappy.sh`** - Installs Snappy compression library
- **`install_yamlcpp.sh`** - Installs YAML-CPP parser
- **`install_protobuf.sh`** - Installs Protocol Buffers

### Parsing and Regex
- **`install_antlr.sh`** - Installs ANTLR4 parser generator
- **`install_pcre2.sh`** - Installs PCRE2 regex library

### Cloud Services
- **`install_aws_sdk.sh`** - Installs AWS SDK C++ and Lambda runtime

### Master Script
- **`install_all.sh`** - Runs all scripts in the correct dependency order

## Usage

### Individual Installation
Each script can be run independently:

```bash
# Install just LLVM
./install_llvm.sh

# Install with custom version
LLVM_VERSION=17.0.0 ./install_llvm.sh

# Install with custom prefix
PREFIX=/usr/local ./install_boost.sh
```

### Complete Installation
Use the master script to install everything:

```bash
./install_all.sh
```

## Docker Build

### Quick Start
Use the provided build script for easy Docker builds:

```bash
# Build for current platform
./build.sh

# Build for specific platform
./build.sh --platform amd64
./build.sh --platform arm64

# Build multi-platform image
./build.sh --platform both --buildx

# Build and push to registry
./build.sh --platform both --buildx --push

# Build with custom installation prefix
./build.sh --prefix /usr/local
```

### Manual Docker Build
Build manually with Docker:

```bash
# Single platform build
docker build --platform linux/amd64 -t tuplex/musl:amd64 .

# Multi-platform build (requires docker buildx)
docker buildx build --platform linux/amd64,linux/arm64 -t tuplex/musl:latest .
```

### Docker Build Optimization
For optimal Docker layer caching, the Dockerfile is structured with separate layers:

```dockerfile
# Base system
RUN /tmp/install_alpine.sh

# Core dependencies  
RUN /tmp/install_llvm.sh && /tmp/install_boost.sh

# Libraries
RUN /tmp/install_zlib.sh && /tmp/install_googletest.sh && /tmp/install_snappy.sh

# Parsing libraries
RUN /tmp/install_yamlcpp.sh && /tmp/install_celero.sh && /tmp/install_antlr.sh

# System libraries
RUN /tmp/install_pcre2.sh && /tmp/install_protobuf.sh

# Cloud services
RUN /tmp/install_aws_sdk.sh
```

### Customizing Versions and Installation Path
Override dependency versions and installation path using Docker build arguments:

```bash
# Custom versions
docker build \
  --build-arg BOOST_VERSION=1.89.0 \
  --build-arg LLVM_VERSION=17.0.0 \
  --build-arg AWSSDK_CPP_VERSION=1.12.0 \
  -t tuplex/musl:custom .

# Custom installation prefix
docker build \
  --build-arg PREFIX=/usr/local \
  -t tuplex/musl:custom-prefix .

# Both custom versions and prefix
docker build \
  --build-arg PREFIX=/usr/local \
  --build-arg BOOST_VERSION=1.89.0 \
  --build-arg LLVM_VERSION=17.0.0 \
  -t tuplex/musl:custom-all .
```

## Environment Variables

All scripts support the following environment variables:

- **`PREFIX`** - Installation prefix (default: `/opt`)
- **`WORKDIR`** - Working directory for downloads (default: `/tmp`)
- **`CC`** - C compiler (default: `gcc`)
- **`CXX`** - C++ compiler (default: `g++`)
- **`CPU_COUNT`** - Number of CPU cores for parallel builds (auto-detected)

### Version-Specific Variables
- **`LLVM_VERSION`** - LLVM version (default: `16.0.6`)
- **`BOOST_VERSION`** - Boost version (default: `1.88.0`)
- **`AWSSDK_CPP_VERSION`** - AWS SDK version (default: `1.11.524`)
- **`ANTLR4_VERSION`** - ANTLR4 version (default: `4.13.1`)
- **`YAML_CPP_VERSION`** - YAML-CPP version (default: `0.8.0`)
- **`CELERO_VERSION`** - Celero version (default: `2.8.3`)
- **`PCRE2_VERSION`** - PCRE2 version (default: `10.45`)
- **`PROTOBUF_VERSION`** - Protocol Buffers version (default: `32.0`)
- **`SNAPPY_VERSION`** - Snappy version (default: `1.2.2`)
- **`ZLIB_VERSION`** - zlib-ng version (default: `2.1.3`)
- **`CMAKE_VERSION`** - CMake version (default: `3.27.5`)

## Dependencies Order

The installation order is important due to dependencies:

1. **Alpine packages + CMake** - Base system and build tools
2. **zlib-ng** - Required by AWS SDK
3. **LLVM** - Compiler infrastructure
4. **Boost** - C++ libraries
5. **Google Test** - Testing framework
6. **Snappy** - Compression library
7. **YAML-CPP** - YAML parsing
8. **Celero** - Benchmarking
9. **ANTLR4** - Parser generator
10. **PCRE2** - Regex library
11. **Protocol Buffers** - Serialization
12. **AWS SDK** - Cloud services (depends on zlib-ng)

## Requirements

- Alpine Linux
- Root privileges (run with `sudo` or as root)
- Internet connection for downloading source code
- Sufficient disk space for builds

## Notes

- All scripts use `set -euxo pipefail` for strict error handling
- Scripts automatically detect CPU count for parallel builds
- Downloads are cleaned up after installation to save space
- All libraries are built with `-fPIC` for position-independent code
- Most libraries are built as static libraries for better portability
- Docker builds are optimized with separate layers for better caching
- Multi-platform builds are supported via docker buildx
