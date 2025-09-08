#!/bin/bash
# Build script for Tuplex musllinux Docker image
# This script demonstrates how to build with different configurations

set -e

# Default values
PLATFORM=""
TAG="tuplex/musl"
PREFIX="/opt"
PUSH=false
BUILDX=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        --tag)
            TAG="$2"
            shift 2
            ;;
        --prefix)
            PREFIX="$2"
            shift 2
            ;;
        --push)
            PUSH=true
            shift
            ;;
        --buildx)
            BUILDX=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --platform PLATFORM    Target platform (amd64, arm64, or both)"
            echo "  --tag TAG              Docker image tag (default: tuplex/musl)"
            echo "  --prefix PREFIX        Installation prefix (default: /opt)"
            echo "  --push                 Push image to registry after build"
            echo "  --buildx               Use docker buildx for multi-platform builds"
            echo "  --help                 Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "Building Tuplex musllinux Docker image..."
echo "Tag: $TAG"
echo "Prefix: $PREFIX"
echo "Platform: ${PLATFORM:-auto-detect}"
echo "Buildx: $BUILDX"
echo "Push: $PUSH"
echo ""

# Function to build single platform
build_single() {
    local platform=$1
    local tag_suffix=$2
    
    echo "Building for platform: $platform"
    docker build --platform linux/$platform \
                 --build-arg PREFIX=$PREFIX \
                 --build-arg BOOST_VERSION=1.88.0 \
                 --build-arg LLVM_VERSION=16.0.6 \
                 --build-arg AWSSDK_CPP_VERSION=1.11.524 \
                 --build-arg ANTLR4_VERSION=4.13.1 \
                 --build-arg YAML_CPP_VERSION=0.8.0 \
                 --build-arg AWS_LAMBDA_CPP_VERSION=0.2.10 \
                 --build-arg PCRE2_VERSION=10.45 \
                 --build-arg PROTOBUF_VERSION=32.0 \
                 --build-arg CELERO_VERSION=2.8.3 \
                 --build-arg SNAPPY_VERSION=1.2.2 \
                 --build-arg ZLIB_VERSION=2.1.3 \
                 --build-arg CMAKE_VERSION=3.27.5 \
                 -t $TAG:$tag_suffix .
    
    if [ "$PUSH" = true ]; then
        echo "Pushing $TAG:$tag_suffix..."
        docker push $TAG:$tag_suffix
    fi
}

# Function to build multi-platform
build_multi() {
    echo "Building multi-platform image..."
    
    # Enable buildx if requested
    if [ "$BUILDX" = true ]; then
        docker buildx create --use --name tuplex-builder || true
    fi
    
    docker buildx build --platform linux/amd64,linux/arm64 \
                       --build-arg PREFIX=$PREFIX \
                       --build-arg BOOST_VERSION=1.88.0 \
                       --build-arg LLVM_VERSION=16.0.6 \
                       --build-arg AWSSDK_CPP_VERSION=1.11.524 \
                       --build-arg ANTLR4_VERSION=4.13.1 \
                       --build-arg YAML_CPP_VERSION=0.8.0 \
                       --build-arg AWS_LAMBDA_CPP_VERSION=0.2.10 \
                       --build-arg PCRE2_VERSION=10.45 \
                       --build-arg PROTOBUF_VERSION=32.0 \
                       --build-arg CELERO_VERSION=2.8.3 \
                       --build-arg SNAPPY_VERSION=1.2.2 \
                       --build-arg ZLIB_VERSION=2.1.3 \
                       --build-arg CMAKE_VERSION=3.27.5 \
                       -t $TAG:latest .
    
    if [ "$PUSH" = true ]; then
        echo "Pushing multi-platform image..."
        docker buildx build --platform linux/amd64,linux/arm64 \
                           --build-arg BOOST_VERSION=1.88.0 \
                           --build-arg LLVM_VERSION=16.0.6 \
                           --build-arg AWSSDK_CPP_VERSION=1.11.524 \
                           --build-arg ANTLR4_VERSION=4.13.1 \
                           --build-arg YAML_CPP_VERSION=0.8.0 \
                           --build-arg AWS_LAMBDA_CPP_VERSION=0.2.10 \
                           --build-arg PCRE2_VERSION=10.45 \
                           --build-arg PROTOBUF_VERSION=32.0 \
                           --build-arg CELERO_VERSION=2.8.3 \
                           --build-arg SNAPPY_VERSION=1.2.2 \
                           --build-arg ZLIB_VERSION=2.1.3 \
                           --build-arg CMAKE_VERSION=3.27.5 \
                           -t $TAG:latest --push .
    fi
}

# Main build logic
if [ -z "$PLATFORM" ]; then
    # Auto-detect platform
    if [ "$BUILDX" = true ]; then
        build_multi
    else
        # Build for current platform
        CURRENT_ARCH=$(uname -m)
        if [ "$CURRENT_ARCH" = "x86_64" ]; then
            build_single "amd64" "latest"
        elif [ "$CURRENT_ARCH" = "aarch64" ] || [ "$CURRENT_ARCH" = "arm64" ]; then
            build_single "arm64" "latest"
        else
            echo "Unsupported architecture: $CURRENT_ARCH"
            exit 1
        fi
    fi
else
    case $PLATFORM in
        "amd64")
            build_single "amd64" "amd64"
            ;;
        "arm64")
            build_single "arm64" "arm64"
            ;;
        "both"|"multi")
            build_multi
            ;;
        *)
            echo "Unsupported platform: $PLATFORM"
            echo "Supported platforms: amd64, arm64, both"
            exit 1
            ;;
    esac
fi

echo ""
echo "Build completed successfully!"
echo "Image: $TAG"

# Show available images
echo ""
echo "Available images:"
docker images | grep $TAG || echo "No images found with tag $TAG"
