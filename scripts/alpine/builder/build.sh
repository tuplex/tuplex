#!/bin/bash

# Alpine wheel builder script
# This script will be executed when the container starts
# Usage: build.sh [--test-only]

set -e

# Check for --test-only parameter
TEST_ONLY=false
if [ "$1" = "--test-only" ]; then
    TEST_ONLY=true
    echo "Test-only mode: will skip wheel building and test existing wheel"
fi

echo "Alpine wheel builder container started"

# Use Python 3.11 from the tuplex/musl image
PYTHON="/opt/_internal/cpython-3.11.13/bin/python3.11"
PIP="/opt/_internal/cpython-3.11.13/bin/pip"

echo "Using Python: $PYTHON"
echo "Python version: $($PYTHON --version)"

    # Check if the project root is mounted
    if [ ! -f "/code/setup.py" ]; then
        echo "Error: /code/setup.py not found. Make sure the project root is mounted."
        exit 1
    fi

    # Check if wheelhouse directory is mounted
    if [ ! -d "/wheelhouse" ]; then
        echo "Error: /wheelhouse directory not found. Make sure the wheelhouse is mounted."
        exit 1
    fi

    # Create a writable build directory in /work (since /code is read-only)
    echo "Setting up writable build directory in /work..."
    mkdir -p /work/build

if [ "$TEST_ONLY" = false ]; then
    echo "Installing required dependencies..."
    $PIP install setuptools wheel cloudpickle numpy attrs dill pluggy py pygments six wcwidth astor prompt_toolkit jedi PyYAML psutil iso8601

    export LLVM_ROOT_DIR="/opt/llvm-16.0.6"
    export LLVM_CONFIG="/opt/llvm-16.0.6/bin/llvm-config"
    export PATH="/opt/llvm-16.0.6/bin:$PATH"

    echo "Building wheel for tuplex..."

    export CMAKE_ARGS="
       -DCMAKE_BUILD_TYPE=RelWithDebInfo \
       -DBUILD_WITH_AWS=OFF \
       -DBUILD_WITH_ORC=OFF \
       -DBUILD_SHARED_LIBS=ON \
       -DSKIP_AWS_TESTS=ON \
       -DBUILD_FOR_CI=ON \
       -DBUILD_TESTING=OFF \
       -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
       -DCMAKE_CXX_FLAGS=\"-O3 -DNDEBUG\" \
       -DCMAKE_C_FLAGS=\"-O3 -DNDEBUG\" \
       -DCMAKE_SHARED_LINKER_FLAGS=\"-Wl,--export-dynamic -Wl,--no-as-needed\" \
       -DLLVM_ROOT=/opt/llvm-16.0.6 \
       -DLLVM_ROOT_DIR=/opt/llvm-16.0.6 \
       -DLLVM_CONFIG=/opt/llvm-16.0.6/bin/llvm-config \
       -DPython3_EXECUTABLE=/opt/_internal/cpython-3.11.13/bin/python3.11 \
       -DPython3_ROOT_DIR=/opt/_internal/cpython-3.11.13 \
       -DPython3_LIBRARY=/opt/_internal/cpython-3.11.13/lib/libpython3.11.so \
       -DPython3_INCLUDE_DIR=/opt/python/cp311-cp311/include/python3.11 \
       -DPYTHON3_VERSION=3.11 \
       -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    "

    # Build the wheel using setup.py
    echo "Building wheel with setup.py..."
    # Exclude history server from the wheel to reduce size and dependencies
    echo "Setting TUPLEX_INCLUDE_HISTORYSERVER=False to exclude history server"
    export TUPLEX_INCLUDE_HISTORYSERVER=False
    echo "Environment variable TUPLEX_INCLUDE_HISTORYSERVER is set to: $TUPLEX_INCLUDE_HISTORYSERVER"
    
    # Change to the source directory and build with writable build directory
    cd /code
    export TMPDIR=/work/tmp
    mkdir -p /work/tmp
    CMAKE_ARGS="$CMAKE_ARGS" $PYTHON setup.py build --build-base /work/build bdist_wheel

    # Find the built wheel
    WHEEL_FILE=$(find dist/ -name "*.whl" | head -1)

    if [ -z "$WHEEL_FILE" ]; then
        echo "Error: No wheel file found in dist/ directory"
        exit 1
    fi

    echo "Built wheel: $WHEEL_FILE"

    check_pyinit_tuplex_symbol() {
        # Check that the symbol PyInit_tuplex is present in the tuplex .so
        echo "Checking for PyInit_tuplex symbol in the built .so..."

        # Find the .so file in the wheel
        local so_file
        so_file=$(unzip -l "$WHEEL_FILE" | grep '\.so$' | awk '{print $4}' | head -1)
        if [ -z "$so_file" ]; then
            echo "Error: No .so file found in the wheel."
            exit 1
        fi

        # Extract the .so file to a temp location
        local tmp_so_dir
        tmp_so_dir=$(mktemp -d)
        unzip -j "$WHEEL_FILE" "$so_file" -d "$tmp_so_dir" >/dev/null

        local so_path="$tmp_so_dir/$(basename "$so_file")"

        if [ ! -f "$so_path" ]; then
            echo "Error: Failed to extract .so file from wheel."
            exit 1
        fi

        # Check for PyInit_tuplex symbol
        if ! nm -D "$so_path" 2>/dev/null | grep -q 'PyInit_tuplex'; then
            echo "Error: PyInit_tuplex symbol not found in $so_path"
            exit 1
        else
            echo "✓ PyInit_tuplex symbol found in $so_path"
        fi

        # Clean up temp .so extraction directory
        rm -rf "$tmp_so_dir"
    }

    check_pyinit_tuplex_symbol

    # Use auditwheel to repair/delocate the wheel
    echo "Repairing wheel with auditwheel..."
    $PIP install auditwheel
    auditwheel repair "$WHEEL_FILE" -w /wheelhouse

    # Find the repaired wheel
    REPAIRED_WHEEL=$(find /wheelhouse -name "*.whl" | head -1)

    if [ -z "$REPAIRED_WHEEL" ]; then
        echo "Error: No repaired wheel found in /wheelhouse"
        exit 1
    fi

    echo "Successfully built and repaired wheel: $REPAIRED_WHEEL"
    echo "Wheel stored in: /wheelhouse"
else
    echo "Skipping wheel building (test-only mode)"
    # Find the existing repaired wheel
    REPAIRED_WHEEL=$(find /wheelhouse -name "*.whl" | head -1)
    
    if [ -z "$REPAIRED_WHEEL" ]; then
        echo "Error: No wheel file found in /wheelhouse for testing"
        exit 1
    fi
    
    echo "Found existing wheel for testing: $REPAIRED_WHEEL"
fi




echo "=============================="
echo "Testing built wheel in a fresh virtual environment..."
echo "=============================="

# Create a temporary directory for the venv
VENV_DIR=$(mktemp -d)
echo "Created temporary venv directory: $VENV_DIR"

# Create a new virtual environment
$PYTHON -m venv "$VENV_DIR/venv"
source "$VENV_DIR/venv/bin/activate"

# Upgrade pip and install wheel
pip install --upgrade pip wheel

# Install the repaired wheel
pip install "$REPAIRED_WHEEL"

# Install test dependencies (pytest, etc.)
pip install pytest

# Run the tests in tuplex/python/tests
echo "Running tests in tuplex/python/tests..."
pytest /code/tuplex/python/tests

TEST_EXIT_CODE=$?

# Deactivate and remove the virtual environment
deactivate
rm -rf "$VENV_DIR"

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "All tests passed successfully!"
else
    echo "Some tests failed. Exit code: $TEST_EXIT_CODE"
    exit $TEST_EXIT_CODE
fi

