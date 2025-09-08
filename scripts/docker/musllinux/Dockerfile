# Build musllinux (Alpine) compatible version.
# You can create a multiplatform docker image via
# docker buildx build --platform linux/amd64,linux/arm64 -t tuplex/musl .

# BUILD INSTRUCTIONS:
# ===================
# 
# Single platform builds:
# -----------------------
# For AMD64 (x86_64) systems:
#   docker build --platform linux/amd64 -t tuplex/musl:amd64 .
#
# For ARM64 (aarch64) systems (like Apple Silicon):
#   docker build --platform linux/arm64 -t tuplex/musl:arm64 .
#
# Multi-platform build (requires docker buildx):
# ---------------------------------------------
#   docker buildx build --platform linux/amd64,linux/arm64 -t tuplex/musl:latest .
#
# To enable buildx (if not already enabled):
#   docker buildx create --use
#
# To see available platforms:
#   docker buildx inspect --bootstrap

# Set platform-specific base image
ARG TARGETPLATFORM
ARG BUILDPLATFORM

# Use appropriate base image based on target platform
FROM quay.io/pypa/musllinux_1_2_x86_64:latest AS base-amd64
FROM quay.io/pypa/musllinux_1_2_aarch64:latest AS base-arm64

# Select the appropriate base image
FROM base-${TARGETPLATFORM#linux/}

# docker build -t tuplex/musl .
# docker run -it tuplex/musl bash

# Installation prefix - all dependencies will be installed to this directory
ARG PREFIX=/opt

# Version arguments for dependencies
ARG BOOST_VERSION=1.88.0
ARG LLVM_VERSION=16.0.6
ARG AWSSDK_CPP_VERSION=1.11.524
ARG ANTLR4_VERSION=4.13.1
ARG YAML_CPP_VERSION=0.8.0
ARG AWS_LAMBDA_CPP_VERSION=0.2.10
ARG PCRE2_VERSION=10.45
ARG PROTOBUF_VERSION=32.0
ARG CELERO_VERSION=2.8.3
ARG SNAPPY_VERSION=1.2.2
ARG ZLIB_VERSION=2.1.3
ARG CMAKE_VERSION=3.27.5

# Update package index
RUN apk update

# Install basic dependencies needed for the scripts
RUN apk add --no-cache \
    bash \
    wget \
    curl \
    git \
    make \
    gcc \
    g++ \
    cmake \
    ninja \
    python3 \
    py3-pip

# Create necessary directories
RUN mkdir -p /opt/sbin /opt/bin /opt/lib /opt/include

# Copy all installation scripts
COPY install_alpine.sh install_zlib.sh install_llvm.sh install_boost.sh \
     install_googletest.sh install_snappy.sh install_zstd.sh install_yamlcpp.sh install_celero.sh \
     install_antlr.sh install_pcre2.sh install_protobuf.sh install_aws_sdk.sh \
     /tmp/

# Make all scripts executable
RUN chmod +x /tmp/install_*.sh

# Set working directory
WORKDIR /opt

# Layer 1: Install Alpine packages and CMake (base system dependencies)
RUN /tmp/install_alpine.sh

# Layer 2: Install zlib-ng (required by AWS SDK)
RUN /tmp/install_zlib.sh

# Layer 3: Install testing and compression libraries
RUN /tmp/install_googletest.sh 

# Layer 4: Install snappy compression library
RUN /tmp/install_snappy.sh

# Layer 5: Install zstd compression library
RUN /tmp/install_zstd.sh

# Layer 6: Install parsing and data libraries
RUN /tmp/install_yamlcpp.sh 

# Layer 7: Install celero benchmarking library
RUN /tmp/install_celero.sh

# Layer 8: Install parsing and regex libraries
RUN /tmp/install_antlr.sh 

# Layer 9: Install parsing and regex library
RUN /tmp/install_pcre2.sh

# Layer 10: Install Protocol Buffers
RUN /tmp/install_protobuf.sh

# Layer 11: Install AWS SDK C++ and Lambda runtime
RUN /tmp/install_aws_sdk.sh

# Layer 12: Install Boost (C++ libraries)
RUN /tmp/install_boost.sh

# Layer 13: Install LLVM (compiler infrastructure) (this will take a while)
RUN /tmp/install_llvm.sh

# Clean up installation scripts
RUN rm -f /tmp/install_*.sh

# Clean up downloaded temporary files and build artifacts
RUN rm -rf /tmp/tuplex-downloads

# Fix CMake issue by using the working pipx-installed version
RUN ln -sf /opt/_internal/pipx/venvs/cmake/bin/cmake /opt/bin/cmake

# Set environment variables
ENV PATH="/opt/bin:/opt/sbin:$PATH"
ENV LD_LIBRARY_PATH="/opt/lib:/opt/lib64:$LD_LIBRARY_PATH"
ENV PKG_CONFIG_PATH="/opt/lib/pkgconfig:$PKG_CONFIG_PATH"

RUN mkdir -p /code

WORKDIR /code

# Default command
CMD ["/bin/bash"]

