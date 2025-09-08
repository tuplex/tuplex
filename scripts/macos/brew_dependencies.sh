#!/usr/bin/env bash
# This script installs all required dependencies via brew
# for instructions on how to install brew, visit https://brew.sh/

set -euo pipefail

# Function to verify a command exists and is working
verify_command() {
    local cmd="$1"
    local expected_output="$2"
    echo "Verifying $cmd..."
    
    if command -v "$cmd" >/dev/null 2>&1; then
        if [ -n "$expected_output" ]; then
            local actual_output
            actual_output=$($cmd --version 2>/dev/null || echo "version check failed")
            echo "$cmd found: $actual_output"
        else
            echo "$cmd found"
        fi
        return 0
    else
        echo "ERROR: $cmd not found in PATH"
        return 1
    fi
}

# Function to verify library exists
verify_library() {
    local lib_name="$1"
    local search_paths="$2"
    echo "Verifying $lib_name library..."
    
    # Expand all paths including wildcards
    local expanded_paths=""
    for path in $search_paths; do
        if [[ "$path" == *"*"* ]]; then
            # Expand wildcards
            for expanded_path in $path; do
                if [ -d "$expanded_path" ]; then
                    expanded_paths="$expanded_paths $expanded_path"
                fi
            done
        else
            expanded_paths="$expanded_paths $path"
        fi
    done
    
    # Search in all expanded paths
    for path in $expanded_paths; do
        echo "  Searching in: $path"
        # Check if directory exists before searching
        if [ ! -d "$path" ]; then
            echo "  Directory does not exist, skipping"
            continue
        fi
        # Use a temporary variable to capture output and check length
        local found_files
        found_files=$(find "$path" -name "*${lib_name}*" -type f 2>/dev/null)
        echo "  Found $(echo "$found_files" | wc -l) files"
        if [ -n "$found_files" ]; then
            echo "$lib_name found in $path"
            echo "$found_files" | head -3
            return 0
        fi
    done
    
    echo "ERROR: $lib_name not found in any of: $search_paths"
    return 1
}

# Function to verify cmake package
verify_cmake_package() {
    local package_name="$1"
    echo "Verifying CMake package: $package_name"
    
    # Get Homebrew paths dynamically
    local homebrew_prefix="${HOMEBREW_PREFIX:-$(brew --prefix)}"
    local homebrew_cellar="${HOMEBREW_CELLAR:-$(brew --cellar)}"
    
    # Search in standard locations and Cellar paths
    # Use find to get all CMake directories
    local expanded_paths="$homebrew_prefix/lib/cmake /usr/local/lib/cmake"
    
    # Add Cellar paths using find to get actual paths
    cellar_cmake_paths=$(find "$homebrew_cellar" -name "cmake" -type d -path "*/lib/cmake" 2>/dev/null)
    if [ -n "$cellar_cmake_paths" ]; then
        expanded_paths="$expanded_paths $cellar_cmake_paths"
    fi
    
    # Search in all expanded paths
    for path in $expanded_paths; do
        # Check if directory exists before searching
        if [ ! -d "$path" ]; then
            continue
        fi
        # Use a temporary variable to capture output and check length
        local found_packages
        found_packages=$(find "$path" -name "*${package_name}*" 2>/dev/null)
        if [ -n "$found_packages" ]; then
            echo "CMake package $package_name found"
            echo "$found_packages" | head -3
            return 0
        fi
    done
    
    echo "ERROR: CMake package $package_name not found"
    return 1
}

# Detect Homebrew paths dynamically
HOMEBREW_PREFIX="$(brew --prefix)"
HOMEBREW_CELLAR="$(brew --cellar)"
ARCH="$(uname -m)"

echo "Detected Homebrew configuration:"
echo "  Architecture: $ARCH"
echo "  Homebrew prefix: $HOMEBREW_PREFIX"
echo "  Homebrew cellar: $HOMEBREW_CELLAR"

# Update brew first
echo "Updating brew..."
brew update --quiet 2>/dev/null || echo "Warning: brew update failed"

# Install dependencies with better error handling
echo "Installing dependencies..."

# Install core dependencies first
CORE_DEPENDENCIES="llvm@15 openjdk@11 cmake coreutils zstd zlib libmagic pcre2 gflags yaml-cpp celero wget googletest libdwarf libelf protobuf boost"

for dep in $CORE_DEPENDENCIES; do
    # Check if package is already installed
    if brew list "$dep" &>/dev/null; then
        version=$(brew list --versions "$dep" | awk '{print $2}')
        echo "$dep $version already installed, skipping."
    else
        if brew install "$dep" 2>/dev/null; then
            version=$(brew list --versions "$dep" | awk '{print $2}')
            echo "$dep $version installed successfully."
        else
            echo "ERROR: Failed to install $dep"
            echo "Attempting to reinstall $dep with --force ..."
            if brew reinstall -f "$dep" 2>/dev/null; then
                version=$(brew list --versions "$dep" | awk '{print $2}')
                echo "$dep $version reinstalled successfully."
            else
                echo "ERROR: Failed to reinstall $dep, continuing..."
            fi
        fi
    fi
done

# Update PATH to include brew binaries
echo "Updating PATH..."
export PATH="$HOMEBREW_PREFIX/bin:/usr/local/bin:$PATH"
echo "Updated PATH: $PATH"

# Comprehensive verification
echo "=== Verifying installations ==="

# Verify essential commands
verify_command "protoc" "libprotoc"
verify_command "cmake" "cmake version"
verify_command "java" "openjdk"
verify_command "pkg-config" "pkg-config"

# Verify protobuf specifically
echo "=== Protobuf verification ==="
if verify_command "protoc" "libprotoc"; then
    echo "Protobuf version: $(protoc --version)"
    echo "Protobuf include path: $(pkg-config --variable=includedir protobuf 2>/dev/null || echo 'not found')"
    echo "Protobuf lib path: $(pkg-config --variable=libdir protobuf 2>/dev/null || echo 'not found')"
else
    echo "ERROR: Protobuf verification failed"
    echo "Searching for protobuf installations..."
    find "$HOMEBREW_PREFIX" /usr/local -name "*protobuf*" 2>/dev/null | head -10
    exit 1
fi

# Verify libraries using dynamic paths
verify_library "protobuf" "$HOMEBREW_PREFIX/lib /usr/local/lib $HOMEBREW_CELLAR/protobuf/*/lib"
verify_library "boost" "$HOMEBREW_PREFIX/lib /usr/local/lib $HOMEBREW_CELLAR/boost/*/lib"
verify_library "llvm" "$HOMEBREW_PREFIX/lib /usr/local/lib $HOMEBREW_CELLAR/llvm@15/*/lib"

# Verify cmake packages
verify_cmake_package "protobuf"
verify_cmake_package "boost"