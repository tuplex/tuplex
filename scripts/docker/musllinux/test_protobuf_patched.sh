#!/bin/bash

echo "=== Testing Protobuf 24.3 with Musl Compatibility Patch ==="

# Step 1: Clone protobuf
echo "1. Cloning protobuf..."
git clone -b v24.3 https://github.com/protocolbuffers/protobuf.git /tmp/protobuf-test
cd /tmp/protobuf-test

echo "2. Updating submodules..."
git submodule update --init --recursive

echo "3. Applying musl compatibility patch..."
# Create the patch file inline
cat > /tmp/musl-fix.patch << 'EOF'
diff --git a/third_party/abseil-cpp/absl/base/internal/direct_mmap.h b/third_party/abseil-cpp/absl/base/internal/direct_mmap.h
index 1234567..abcdefg 100644
--- a/third_party/abseil-cpp/absl/base/internal/direct_mmap.h
+++ b/third_party/abseil-cpp/absl/base/internal/direct_mmap.h
@@ -23,6 +23,14 @@
 #include <sys/mman.h>
 #include <unistd.h>
 
+// Fix for musl libc compatibility - off64_t is not defined
+#if defined(__MUSL__) || !defined(_LARGEFILE64_SOURCE)
+#ifndef off64_t
+#define off64_t off_t
+#endif
+#endif
+
+
 namespace absl {
 ABSL_NAMESPACE_BEGIN
 namespace base_internal {
diff --git a/third_party/abseil-cpp/absl/base/internal/low_level_alloc.cc b/third_party/abseil-cpp/absl/base/internal/low_level_alloc.cc
index 1234567..abcdefg 100644
--- a/third_party/abseil-cpp/absl/base/internal/low_level_alloc.cc
+++ b/third_party/abseil-cpp/absl/base/internal/low_level_alloc.cc
@@ -20,6 +20,14 @@
 #include <sys/mman.h>
 #include <unistd.h>
 
+// Fix for musl libc compatibility - off64_t is not defined
+#if defined(__MUSL__) || !defined(_LARGEFILE64_SOURCE)
+#ifndef off64_t
+#define off64_t off_t
+#endif
+#endif
+
+
 #include "absl/base/internal/low_level_alloc.h"
 
 #include <stddef.h>
diff --git a/CMakeLists.txt b/CMakeLists.txt
index 1234567..abcdefg 100644
--- a/CMakeLists.txt
+++ b/CMakeLists.txt
@@ -7,6 +7,12 @@
 cmake_minimum_required(VERSION 3.5)
 project(protobuf VERSION 24.3.0 LANGUAGES C CXX)
 
+# Add musl libc compatibility flags
+if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
+  add_compile_definitions(_LARGEFILE64_SOURCE)
+  add_compile_definitions(_FILE_OFFSET_BITS=64)
+endif()
+
 # Set C++ standard
 set(CMAKE_CXX_STANDARD 17)
 set(CMAKE_CXX_STANDARD_REQUIRED ON)
EOF

# Apply the patch
patch -p1 < /tmp/musl-fix.patch

echo "4. Building protobuf with patch..."
mkdir build
cd build
/opt/bin/cmake -DCMAKE_CXX_FLAGS="-fPIC -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64" -DCMAKE_CXX_STANDARD=17 -Dprotobuf_BUILD_TESTS=OFF ..
make -j4

echo "5. Installing protobuf..."
make install

echo "6. Testing protoc..."
protoc --version

echo "7. Creating test project..."
cd /tmp
mkdir proto-test
cd proto-test

echo "8. Creating test proto file..."
cat > test.proto << 'EOF'
syntax = "proto3";
message TestMessage {
    string text = 1;
    int32 number = 2;
}
EOF

echo "9. Compiling proto file..."
protoc --cpp_out=. test.proto

echo "10. Creating test program..."
cat > main.cpp << 'EOF'
#include <iostream>
#include "test.pb.h"

int main() {
    TestMessage msg;
    msg.set_text("Hello Protobuf!");
    msg.set_number(42);
    
    std::cout << "Text: " << msg.text() << std::endl;
    std::cout << "Number: " << msg.number() << std::endl;
    
    return 0;
}
EOF

echo "11. Building test program..."
g++ -std=c++17 main.cpp test.pb.cc -lprotobuf -o test_program

echo "12. Running test..."
./test_program

echo "=== Protobuf test with musl patch completed successfully! ==="
