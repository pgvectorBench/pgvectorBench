include(FetchContent)

# Keep dependency sources independent of Git submodules. Released dependencies
# are pinned to upstream tags, while Ryu is pinned to a master commit.
set(PGVECTORBENCH_ARGPARSE_VERSION "3.2")
set(PGVECTORBENCH_ARGPARSE_SHA256
    "9dcb3d8ce0a41b2a48ac8baa54b51a9f1b6a2c52dd374e28cc713bab0568ec98")
set(PGVECTORBENCH_CONCURRENTQUEUE_VERSION "1.0.5")
set(PGVECTORBENCH_CONCURRENTQUEUE_SHA256
    "4d6368a27492d86011fde5ca0cf386dce7c49cd425aa3d9b063ca6ec373a6ef3")
set(PGVECTORBENCH_SPDLOG_VERSION "1.17.0")
set(PGVECTORBENCH_SPDLOG_SHA256
    "d8862955c6d74e5846b3f580b1605d2428b11d97a410d86e2fb13e857cd3a744")

# URL archives should use extraction-time timestamps so a newly downloaded
# dependency is rebuilt when necessary. CMP0135 is available from CMake 3.24.
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

# Do not build dependency-owned examples, tests, or benchmarks as part of
# pgvectorBench, and disable dependency install rules where upstream supports it.
set(ARGPARSE_INSTALL OFF CACHE BOOL "" FORCE)
set(ARGPARSE_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ARGPARSE_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_EXAMPLE_HO OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_TESTS_HO OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_BENCH OFF CACHE BOOL "" FORCE)
set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  argparse
  URL
    "https://github.com/p-ranav/argparse/archive/refs/tags/v${PGVECTORBENCH_ARGPARSE_VERSION}.tar.gz"
  URL_HASH "SHA256=${PGVECTORBENCH_ARGPARSE_SHA256}")

FetchContent_Declare(
  concurrentqueue
  URL
    "https://github.com/cameron314/concurrentqueue/archive/refs/tags/v${PGVECTORBENCH_CONCURRENTQUEUE_VERSION}.tar.gz"
  URL_HASH "SHA256=${PGVECTORBENCH_CONCURRENTQUEUE_SHA256}")

FetchContent_Declare(
  ryu
  GIT_REPOSITORY https://github.com/ulfjack/ryu.git
  GIT_TAG 4c0618b0e44f7ef027ebae05d2cc7812048f7c8f
  GIT_PROGRESS TRUE)

FetchContent_Declare(
  spdlog
  URL
    "https://github.com/gabime/spdlog/archive/refs/tags/v${PGVECTORBENCH_SPDLOG_VERSION}.tar.gz"
  URL_HASH "SHA256=${PGVECTORBENCH_SPDLOG_SHA256}")

FetchContent_MakeAvailable(argparse concurrentqueue ryu spdlog)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_definitions(
    spdlog
    INTERFACE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
  target_compile_definitions(
    spdlog_header_only
    INTERFACE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
endif()
