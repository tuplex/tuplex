#!/usr/bin/env bash

set -euo pipefail

PREFIX=${PREFIX:-/usr/local}
AWSSDK_CPP_VERSION=1.11.524 # need at least 1.11.267 because of pyarrow bugs...

# Check if AWS SDK is already installed
if [ -d "${PREFIX}/include/aws" ]; then
  echo "AWS SDK C++ already installed, skipping."
  exit 0
fi

echo "Installing AWS SDK C++ v${AWSSDK_CPP_VERSION} from source..."

# Get system info
CPU_CORES=$(sysctl -n hw.physicalcpu)
MACOS_VERSION=$(sw_vers -productVersion)
MACOS_VERSION_MAJOR=$(echo "$MACOS_VERSION" | awk -F \. {'print $1'})

# Determine minimum deployment target
if (( MACOS_VERSION_MAJOR >= 11 )); then
    MINIMUM_TARGET="-DCMAKE_OSX_DEPLOYMENT_TARGET=${MACOS_VERSION_MAJOR}.0"
    echo "  Using macOS ${MACOS_VERSION_MAJOR}.0 as minimum target"
else
    MINIMUM_TARGET="-DCMAKE_OSX_DEPLOYMENT_TARGET=10.13"
    echo "  Using macOS 10.13 as minimum target"
fi

# Create temporary directory
TEMP_DIR="/tmp/aws-sdk-cpp-$$"
mkdir -p "$TEMP_DIR"
cd "$TEMP_DIR"

# Cleanup function
cleanup() {
    echo "Cleaning up temporary files..."
    cd /tmp
    rm -rf "$TEMP_DIR" 2>/dev/null || true
}
trap cleanup EXIT

echo "Cloning AWS SDK C++ repository..."
if ! git clone --recurse-submodules --quiet https://github.com/aws/aws-sdk-cpp.git; then
    echo "ERROR: Failed to clone AWS SDK C++ repository"
    exit 1
fi

cd aws-sdk-cpp

echo "Checking out version ${AWSSDK_CPP_VERSION}..."
if ! git checkout --quiet "tags/${AWSSDK_CPP_VERSION}"; then
    echo "ERROR: Failed to checkout version ${AWSSDK_CPP_VERSION}"
    exit 1
fi

echo "Configuring build..."
mkdir -p build
cd build

CMAKE_ARGS=(
    ${MINIMUM_TARGET}
    "-DCMAKE_INSTALL_PREFIX=${PREFIX}"
    "-DCMAKE_BUILD_TYPE=Release"
    "-DUSE_OPENSSL=ON"
    "-DENABLE_TESTING=OFF"
    "-DENABLE_UNITY_BUILD=ON"
    "-DCPP_STANDARD=17"
    "-DBUILD_SHARED_LIBS=OFF"
    "-DBUILD_ONLY=s3;core;lambda;transfer"
    ".."
)

if ! cmake "${CMAKE_ARGS[@]}" >/dev/null 2>&1; then
    echo "ERROR: CMake configuration failed"
    echo "CMake arguments: ${CMAKE_ARGS[*]}"
    exit 1
fi

echo "Building AWS SDK C++ (using ${CPU_CORES} cores)..."
echo "  This may take several minutes. Building in progress..."

# Function to show progress dots
show_progress() {
    local pid=$1
    local dots=0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 10
        dots=$((dots + 1))
        if [ $dots -eq 1 ]; then
            echo "  Still building... (10s elapsed)"
        elif [ $dots -eq 3 ]; then
            echo "  Still building... (30s elapsed)"
        elif [ $dots -eq 6 ]; then
            echo "  Still building... (1m elapsed)"
        elif [ $dots -eq 12 ]; then
            echo "  Still building... (2m elapsed)"
        elif [ $dots -eq 18 ]; then
            echo "  Still building... (3m elapsed)"
        elif [ $dots -eq 30 ]; then
            echo "  Still building... (5m elapsed)"
        elif [ $((dots % 12)) -eq 0 ]; then
            echo "  Still building... ($((dots * 10 / 60))m elapsed)"
        fi
    done
}

# Start the build in background and show progress
make -j"${CPU_CORES}" >/dev/null 2>&1 &
BUILD_PID=$!

# Show progress while building
show_progress $BUILD_PID

# Wait for build to complete and check result
wait $BUILD_PID
BUILD_EXIT_CODE=$?

if [ $BUILD_EXIT_CODE -ne 0 ]; then
    echo "ERROR: Build failed"
    exit 1
fi

echo "  Build completed successfully!"

echo "Installing AWS SDK C++..."
if ! make install >/dev/null 2>&1; then
    echo "ERROR: Installation failed"
    exit 1
fi

echo "AWS SDK C++ v${AWSSDK_CPP_VERSION} installed successfully!"

