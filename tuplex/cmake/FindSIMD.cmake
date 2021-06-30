# contains macros do detect various SIMD instructions sets (x86)
include(CheckCXXSourceCompiles)
include(CheckCXXSourceRuns)


# SSE4.2 => i.e. fast string SIMD instructions
macro(CHECK_FOR_SSE42)
    # Check for SSE4.2 support in the compiler.
    set(OLD_CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS})
    if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} /arch:AVX")
    else(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -msse4.2")
    endif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    check_cxx_source_runs("
#if defined(_MSC_VER)
#include <intrin.h>
#else  // !defined(_MSC_VER)
#include <cpuid.h>
#include <nmmintrin.h>
#endif  // defined(_MSC_VER)
int main() {
  _mm_crc32_u8(0, 0); _mm_crc32_u32(0, 0);
#if defined(_M_X64) || defined(__x86_64__)
   _mm_crc32_u64(0, 0);
#endif // defined(_M_X64) || defined(__x86_64__)
  return 0;
}
"  HAVE_SSE42)
    set(CMAKE_REQUIRED_FLAGS ${OLD_CMAKE_REQUIRED_FLAGS})
endmacro()

# AVX
macro(CHECK_FOR_AVX)
    # Check for SSE4.2 support in the compiler.
    set(OLD_CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS})
    if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} /arch:AVX")
    else()
        set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -mavx")
    endif()
    check_cxx_source_runs("
    #include <immintrin.h>
    int main()
    {
      __m256 a, b, c;
      const float src[8] = { 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f };
      float dst[8];
      a = _mm256_loadu_ps( src );
      b = _mm256_loadu_ps( src );
      c = _mm256_add_ps( a, b );
      _mm256_storeu_ps( dst, c );
      for( int i = 0; i < 8; i++ ){
        if( ( src[i] + src[i] ) != dst[i] ){
          return -1;
        }
      }
      return 0;
    }"
    HAVE_AVX)
    set(CMAKE_REQUIRED_FLAGS ${OLD_CMAKE_REQUIRED_FLAGS})
endmacro()
