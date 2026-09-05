include(FetchContent)

# Keep dependency sources independent of Git submodules. Released dependencies
# are pinned to upstream tags, while Ryu is pinned to a master commit.
option(PGVECTORBENCH_USE_SYSTEM_ARROW
       "Use system-installed Arrow and Parquet libraries" OFF)

set(PGVECTORBENCH_ARROW_VERSION "25.0.1")
set(PGVECTORBENCH_ARROW_COMMIT
    "beccec0d0c451b7aa3e4530416ac431b3c035c69")
set(PGVECTORBENCH_ARROW_SHA256
    "43d5de0a581f43cf63a2c06b4dcf13b9ff6fcd800f023324596e5781093bc500")
set(PGVECTORBENCH_ARGPARSE_VERSION "3.2")
set(PGVECTORBENCH_ARGPARSE_SHA256
    "9dcb3d8ce0a41b2a48ac8baa54b51a9f1b6a2c52dd374e28cc713bab0568ec98")
set(PGVECTORBENCH_CONCURRENTQUEUE_VERSION "1.0.5")
set(PGVECTORBENCH_CONCURRENTQUEUE_SHA256
    "4d6368a27492d86011fde5ca0cf386dce7c49cd425aa3d9b063ca6ec373a6ef3")
set(PGVECTORBENCH_SPDLOG_VERSION "1.17.0")
set(PGVECTORBENCH_SPDLOG_SHA256
    "d8862955c6d74e5846b3f580b1605d2428b11d97a410d86e2fb13e857cd3a744")
set(PGVECTORBENCH_NLOHMANN_JSON_VERSION "3.12.0")
set(PGVECTORBENCH_NLOHMANN_JSON_SHA256
    "42f6e95cad6ec532fd372391373363b62a14af6d771056dbfc86160e6dfff7aa")
set(PGVECTORBENCH_GOOGLETEST_VERSION "1.18.0")
set(PGVECTORBENCH_GOOGLETEST_SHA256
    "6e3191c1455468b3fc35a417fb565c1c5071aee1b7e7f85e30cf48a98d37d8b5")

# URL archives should use extraction-time timestamps so a newly downloaded
# dependency is rebuilt when necessary. CMP0135 is available from CMake 3.24.
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

add_library(pgvectorbench_arrow INTERFACE)
add_library(pgvectorbench::arrow ALIAS pgvectorbench_arrow)
add_library(pgvectorbench_parquet INTERFACE)
add_library(pgvectorbench::parquet ALIAS pgvectorbench_parquet)

if(PGVECTORBENCH_USE_SYSTEM_ARROW)
  find_package(Arrow CONFIG REQUIRED)
  find_package(Parquet CONFIG REQUIRED)

  target_link_libraries(pgvectorbench_arrow INTERFACE Arrow::arrow_shared)
  target_link_libraries(
    pgvectorbench_parquet
    INTERFACE Parquet::parquet_shared pgvectorbench_arrow)
else()
  # Arrow disables its option declarations when used as a subproject unless
  # explicitly requested. Build only the static Arrow and Parquet libraries,
  # with all of their dependencies isolated in the build directory.
  set(_pgvectorbench_build_testing "${BUILD_TESTING}")
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)

  set(ARROW_DEFINE_OPTIONS ON CACHE BOOL "" FORCE)
  set(ARROW_DEPENDENCY_SOURCE BUNDLED CACHE STRING "" FORCE)
  set(ARROW_DEPENDENCY_USE_SHARED OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_STATIC ON CACHE BOOL "" FORCE)
  set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_INTEGRATION OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)
  set(ARROW_GIT_ID "${PGVECTORBENCH_ARROW_COMMIT}" CACHE STRING "" FORCE)
  set(ARROW_GIT_DESCRIPTION "apache-arrow-${PGVECTORBENCH_ARROW_VERSION}"
      CACHE STRING "" FORCE)
  set(ARROW_PARQUET ON CACHE BOOL "" FORCE)
  set(ARROW_MIMALLOC OFF CACHE BOOL "" FORCE)
  set(ARROW_JEMALLOC OFF CACHE BOOL "" FORCE)
  # The supported Zilliz benchmark Parquet files use ZSTD. Keep Arrow's other
  # optional codecs disabled to avoid vendoring dependencies the benchmark
  # does not need.
  set(ARROW_WITH_ZSTD ON CACHE BOOL "" FORCE)

  FetchContent_Declare(
    arrow
    URL
      "https://archive.apache.org/dist/arrow/arrow-${PGVECTORBENCH_ARROW_VERSION}/apache-arrow-${PGVECTORBENCH_ARROW_VERSION}.tar.gz"
    URL_HASH "SHA256=${PGVECTORBENCH_ARROW_SHA256}"
    SOURCE_SUBDIR cpp)
  FetchContent_MakeAvailable(arrow)
  set_property(DIRECTORY "${arrow_SOURCE_DIR}/cpp" PROPERTY EXCLUDE_FROM_ALL TRUE)

  # Some of Arrow's bundled dependencies overwrite these parent cache
  # variables. Restore them before configuring the rest of pgvectorBench.
  set(BUILD_TESTING "${_pgvectorbench_build_testing}" CACHE BOOL
      "Build the testing tree" FORCE)
  set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)
  unset(_pgvectorbench_build_testing)

  target_include_directories(
    pgvectorbench_arrow
    SYSTEM INTERFACE
      "$<BUILD_INTERFACE:${arrow_SOURCE_DIR}/cpp/src>"
      "$<BUILD_INTERFACE:${arrow_BINARY_DIR}/src>")
  target_link_libraries(pgvectorbench_arrow INTERFACE arrow_static)
  target_link_libraries(
    pgvectorbench_parquet
    INTERFACE parquet_static pgvectorbench_arrow)
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
set(JSON_BuildTests OFF CACHE BOOL "" FORCE)
set(JSON_Install OFF CACHE BOOL "" FORCE)

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

FetchContent_Declare(
  nlohmann_json
  URL
    "https://github.com/nlohmann/json/releases/download/v${PGVECTORBENCH_NLOHMANN_JSON_VERSION}/json.tar.xz"
  URL_HASH "SHA256=${PGVECTORBENCH_NLOHMANN_JSON_SHA256}")

FetchContent_MakeAvailable(argparse concurrentqueue ryu spdlog nlohmann_json)

if(BUILD_TESTING)
  set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(
    googletest
    URL
      "https://github.com/google/googletest/archive/refs/tags/v${PGVECTORBENCH_GOOGLETEST_VERSION}.tar.gz"
    URL_HASH "SHA256=${PGVECTORBENCH_GOOGLETEST_SHA256}")
  FetchContent_MakeAvailable(googletest)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_definitions(
    spdlog
    INTERFACE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
  target_compile_definitions(
    spdlog_header_only
    INTERFACE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)
endif()
