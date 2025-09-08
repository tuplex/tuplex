#!/bin/bash
# Test script to verify PREFIX variable configuration in all installation scripts

set -e

echo "Testing PREFIX variable configuration in installation scripts..."
echo "============================================================="

# Test PREFIX variable definition
echo "1. Checking PREFIX variable definition..."
for script in install_*.sh; do
    if [ "$script" = "install_all.sh" ]; then
        continue  # Skip the master script
    fi
    
    if grep -q "PREFIX=\${PREFIX:-/opt}" "$script"; then
        echo "✅ $script: PREFIX variable properly defined"
    else
        echo "❌ $script: PREFIX variable missing or incorrectly defined"
        exit 1
    fi
done

echo ""
echo "2. Checking cmake installations use -DCMAKE_INSTALL_PREFIX..."
CMAKE_SCRIPTS=(
    "install_zlib.sh"
    "install_googletest.sh"
    "install_snappy.sh"
    "install_yamlcpp.sh"
    "install_celero.sh"
    "install_antlr.sh"
    "install_protobuf.sh"
    "install_aws_sdk.sh"
)

for script in "${CMAKE_SCRIPTS[@]}"; do
    if grep -q "CMAKE_INSTALL_PREFIX.*PREFIX" "$script"; then
        echo "✅ $script: cmake uses PREFIX variable"
    else
        echo "❌ $script: cmake missing PREFIX variable"
        exit 1
    fi
done

echo ""
echo "3. Checking configure installations use --prefix..."
CONFIGURE_SCRIPTS=(
    "install_pcre2.sh"
)

for script in "${CONFIGURE_SCRIPTS[@]}"; do
    if grep -q "prefix.*PREFIX" "$script"; then
        echo "✅ $script: configure uses PREFIX variable"
    else
        echo "❌ $script: configure missing PREFIX variable"
        exit 1
    fi
done

echo ""
echo "4. Checking bootstrap installations use --prefix..."
BOOTSTRAP_SCRIPTS=(
    "install_boost.sh"
)

for script in "${BOOTSTRAP_SCRIPTS[@]}"; do
    if grep -q "prefix.*PREFIX" "$script"; then
        echo "✅ $script: bootstrap uses PREFIX variable"
    else
        echo "❌ $script: bootstrap missing PREFIX variable"
        exit 1
    fi
done

echo ""
echo "5. Checking LLVM installation path..."
if grep -q "CMAKE_INSTALL_PREFIX.*PREFIX.*llvm" install_llvm.sh; then
    echo "✅ install_llvm.sh: Uses PREFIX variable for LLVM installation"
else
    echo "❌ install_llvm.sh: LLVM installation path not using PREFIX variable"
    exit 1
fi

echo ""
echo "All PREFIX variable configurations are correct! 🎉"
echo ""
echo "Summary of installation methods:"
echo "  ✅ cmake: -DCMAKE_INSTALL_PREFIX=\${PREFIX}"
echo "  ✅ configure: --prefix=\${PREFIX}"
echo "  ✅ bootstrap: --prefix=\"\${PREFIX}\""
echo "  ✅ LLVM: \${PREFIX}/llvm-\${LLVM_VERSION}"
echo ""
echo "All scripts will now install to the directory specified by the PREFIX"
echo "environment variable (default: /opt) instead of hardcoded paths."
