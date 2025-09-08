#!/usr/bin/env bash
# (c) 2017 - 2023 Tuplex team
# this script invokes the cibuildwheel process with necessary env variables to build musl wheels for linux/docker
# builds wheels for python 3.11+ with musl support
# supports building for amd64, arm64, or both architectures

# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

# Parse command line arguments
TARGET_ARCH=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --arch)
            TARGET_ARCH="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --arch ARCH    Target architecture: amd64, arm64, or both (default: native)"
            echo "  --help, -h     Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Build for native architecture"
            echo "  $0 --arch amd64      # Build for AMD64 only"
            echo "  $0 --arch arm64      # Build for ARM64 only"
            echo "  $0 --arch both       # Build for both architectures"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Determine target architecture
if [[ -z "$TARGET_ARCH" ]]; then
    # Auto-detect native architecture
    NATIVE_ARCH=$(uname -m)
    case "$NATIVE_ARCH" in
        x86_64)
            TARGET_ARCH="amd64"
            ;;
        aarch64|arm64)
            TARGET_ARCH="arm64"
            ;;
        *)
            echo "Unsupported native architecture: $NATIVE_ARCH"
            echo "Please specify --arch explicitly (amd64, arm64, or both)"
            exit 1
            ;;
    esac
    echo "No architecture specified, using native architecture: $TARGET_ARCH"
fi

# Validate architecture choice
case "$TARGET_ARCH" in
    amd64|arm64|both)
        echo "Target architecture: $TARGET_ARCH"
        ;;
    *)
        echo "Invalid architecture: $TARGET_ARCH"
        echo "Supported architectures: amd64, arm64, both"
        exit 1
        ;;
esac

echo "Executing musl buildwheel script located in $CWD"
pushd $CWD > /dev/null
cd ..

# delete dir if exists
rm -rf wheelhouse
# delete in tree build files
rm -rf tuplex/python/tuplex/libexec/tuplex*.so

# Check if musl Docker image exists, build if not
echo "Checking for tuplex/musl Docker image..."
if ! docker image inspect tuplex/musl:latest >/dev/null 2>&1; then
    echo "tuplex/musl:latest image not found."
    echo ""
    case "$TARGET_ARCH" in
        both)
            echo "To build the tuplex/musl Docker image for both architectures, run:"
            echo "    cd scripts/docker/musllinux"
            echo "    ./build.sh --platform both --buildx"
            ;;
        amd64)
            echo "To build the tuplex/musl Docker image for AMD64, run:"
            echo "    cd scripts/docker/musllinux"
            echo "    ./build.sh --platform amd64"
            ;;
        arm64)
            echo "To build the tuplex/musl Docker image for ARM64, run:"
            echo "    cd scripts/docker/musllinux"
            echo "    ./build.sh --platform arm64"
            ;;
    esac
    echo ""
    echo "After building the image, re-run this script."
    exit 1
else
    echo "tuplex/musl:latest image found"
    if [[ "$TARGET_ARCH" == "both" ]]; then
        echo "Note: For multi-architecture builds, ensure the image supports both x86_64 and aarch64"
    fi
fi

# CIBUILDWHEEL CONFIGURATION
export CIBUILDWHEEL=1
export TUPLEX_BUILD_ALL=0

# Set architecture-specific configuration
case "$TARGET_ARCH" in
    amd64)
        export CIBW_ARCHS_LINUX="x86_64"
        export CIBW_MUSLLINUX_X86_64_IMAGE='tuplex/musl:latest'
        ;;
    arm64)
        export CIBW_ARCHS_LINUX="aarch64"
        export CIBW_MUSLLINUX_AARCH64_IMAGE='tuplex/musl:latest'
        ;;
    both)
        export CIBW_ARCHS_LINUX="x86_64 aarch64"
        export CIBW_MUSLLINUX_X86_64_IMAGE='tuplex/musl:latest'
        export CIBW_MUSLLINUX_AARCH64_IMAGE='tuplex/musl:latest'
        ;;
esac

# check whether lambda zip was build and stored in build-lambda
TUPLEX_LAMBDA_ZIP=${TUPLEX_LAMBDA_ZIP:-build-lambda/tplxlam.zip}

echo "work dir is: $(pwd)"
if [[ -f "${TUPLEX_LAMBDA_ZIP}" ]]; then
	echo "Found lambda runner ${TUPLEX_LAMBDA_ZIP}, adding to package"
	mkdir -p tuplex/other
	cp ${TUPLEX_LAMBDA_ZIP} tuplex/other/tplxlam.zip
fi

# add to environment, e.g. TUPLEX_BUILD_TYPE=tsan to force a tsan build. Release is the default mode
# Note: The tuplex/musl image uses /opt as the installation prefix for both architectures
export CIBW_ENVIRONMENT="CMAKE_ARGS='-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON' LD_LIBRARY_PATH=/opt/lib:/opt/lib64"

# Build only musllinux wheels for Python 3.11+
export CIBW_BUILD="cp3{11,12,13}-*"
# Note: CIBW_ARCHS_LINUX is already set above based on TARGET_ARCH

# Only build musllinux wheels (skip manylinux)
export CIBW_SKIP="*-manylinux_*"

export CIBW_BUILD_VERBOSITY=3
export CIBW_PROJECT_REQUIRES_PYTHON=">=3.11"

# Set platform tag for musl wheels
export CIBW_PLATFORM_LINUX="musllinux_1_2"

echo "Building musl wheels with the following configuration:"
echo "  - Image: tuplex/musl:latest"
echo "  - Python versions: 3.11, 3.12, 3.13"
case "$TARGET_ARCH" in
    amd64)
        echo "  - Architecture: x86_64 (AMD64)"
        ;;
    arm64)
        echo "  - Architecture: aarch64 (ARM64)"
        ;;
    both)
        echo "  - Architectures: x86_64 (AMD64), aarch64 (ARM64)"
        ;;
esac
echo "  - Platform: musllinux_1_2"
echo "  - Skip: manylinux wheels"
echo ""

cibuildwheel --platform linux .

popd > /dev/null

echo "Done building musl wheels!"
echo "Check the wheelhouse directory for the generated .whl files"
