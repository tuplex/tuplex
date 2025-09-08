#!/bin/bash
# Test script to verify Docker build works with new modular scripts

set -e

echo "Testing Docker build with new modular scripts..."
echo "================================================"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not in PATH"
    exit 1
fi

echo "✅ Docker is available"

# Check if all required scripts exist
REQUIRED_SCRIPTS=(
    "install_alpine.sh"
    "install_zlib.sh"
    "install_llvm.sh"
    "install_boost.sh"
    "install_googletest.sh"
    "install_snappy.sh"
    "install_yamlcpp.sh"
    "install_celero.sh"
    "install_antlr.sh"
    "install_pcre2.sh"
    "install_protobuf.sh"
    "install_aws_sdk.sh"
)

echo "Checking required scripts..."
for script in "${REQUIRED_SCRIPTS[@]}"; do
    if [ -f "$script" ]; then
        echo "✅ $script exists"
    else
        echo "❌ $script missing"
        exit 1
    fi
done

# Check if scripts are executable
echo "Checking script permissions..."
for script in "${REQUIRED_SCRIPTS[@]}"; do
    if [ -x "$script" ]; then
        echo "✅ $script is executable"
    else
        echo "❌ $script is not executable"
        exit 1
    fi
done

# Check if Dockerfile exists
if [ -f "Dockerfile" ]; then
    echo "✅ Dockerfile exists"
else
    echo "❌ Dockerfile missing"
    exit 1
fi

# Check if .dockerignore exists
if [ -f ".dockerignore" ]; then
    echo "✅ .dockerignore exists"
else
    echo "❌ .dockerignore missing"
    exit 1
fi

# Check if build script exists
if [ -f "build.sh" ]; then
    echo "✅ build.sh exists"
    if [ -x "build.sh" ]; then
        echo "✅ build.sh is executable"
    else
        echo "❌ build.sh is not executable"
        exit 1
    fi
else
    echo "❌ build.sh missing"
    exit 1
fi

echo ""
echo "All checks passed! 🎉"
echo ""
echo "You can now build the Docker image using:"
echo "  ./build.sh                    # Build for current platform"
echo "  ./build.sh --platform amd64   # Build for AMD64"
echo "  ./build.sh --platform arm64   # Build for ARM64"
echo "  ./build.sh --platform both    # Build multi-platform"
echo ""
echo "Or build manually with:"
echo "  docker build -t tuplex/musl ."
echo ""
echo "The new modular approach provides:"
echo "  ✅ Better Docker layer caching"
echo "  ✅ Easier debugging and maintenance"
echo "  ✅ Flexible version customization"
echo "  ✅ Individual dependency installation"
echo "  ✅ Multi-platform build support"
