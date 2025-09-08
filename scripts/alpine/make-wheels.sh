#!/bin/bash

# Script to build Tuplex wheels for musl-based Alpine Linux images
# This script checks for the required tuplex/musl Docker image and provides
# instructions if it doesn't exist.
# Usage: make-wheels.sh [--test-only]

set -e

# Check for --test-only parameter
TEST_ONLY=false
if [ "$1" = "--test-only" ]; then
    TEST_ONLY=true
    echo "Test-only mode: will test existing wheel without rebuilding"
fi

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "Checking for tuplex/musl Docker image..."

# Check if the tuplex/musl image exists locally
if ! docker image inspect tuplex/musl >/dev/null 2>&1; then
    echo -e "${RED}Error: tuplex/musl Docker image not found locally.${NC}"
    echo ""
    echo -e "${YELLOW}To build the required tuplex/musl image, please run:${NC}"
    echo ""
    echo "  docker build -t tuplex/musl -f scripts/docker/musllinux/Dockerfile ."
    echo ""
    echo "This will create the musl-based image needed for building Alpine-compatible wheels."
    echo ""
    echo -e "${YELLOW}Note:${NC} The build process may take some time as it needs to compile"
    echo "Tuplex and all its dependencies for the musl libc environment."
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ tuplex/musl Docker image found locally.${NC}"
echo "Proceeding with wheel building..."



# Build the tuplex/alpine-wheel-builder image based on scripts/alpine/builder/Dockerfile
echo "Building tuplex/alpine-wheel-builder Docker image..."
docker build -t tuplex/alpine-wheel-builder -f "$SCRIPT_DIR/builder/Dockerfile" "$SCRIPT_DIR/builder"

echo -e "${GREEN}✓ tuplex/alpine-wheel-builder Docker image built successfully.${NC}"

# Prepare Docker mount options
# Mount the entire project root to /code so setup.py and other files are accessible
# Note: Must be writable as setup.py needs to create directories during build
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")/.."
MOUNT_OPTS="-v $PROJECT_ROOT:/code:ro"

echo "Launching tuplex/alpine-wheel-builder container with tuplex/ mounted..."

# Ensure wheelhouse/ exists in the project root directory
if [ ! -d "$SCRIPT_DIR/wheelhouse" ]; then
    echo "Creating wheelhouse/ directory for output wheels..."
    mkdir -p "$SCRIPT_DIR/wheelhouse"
fi

# Add wheelhouse mount to Docker options
MOUNT_OPTS="$MOUNT_OPTS -v $SCRIPT_DIR/wheelhouse:/wheelhouse"


# You can add --rm to auto-remove the container after exit
if [ "$TEST_ONLY" = true ]; then
    echo "Running container in test-only mode..."
    docker run -it --rm \
        $MOUNT_OPTS \
        --name tuplex-alpine-wheel-builder \
        tuplex/alpine-wheel-builder --test-only
else
    echo "Running container in build mode..."
    docker run -it --rm \
        $MOUNT_OPTS \
        --name tuplex-alpine-wheel-builder \
        tuplex/alpine-wheel-builder
fi

echo "Container execution completed."
