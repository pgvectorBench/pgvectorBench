cmake_minimum_required(VERSION 3.23.1)

set(CMAKE_CXX_STANDARD 17)

SET(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

if(APPLE)
    set(CMAKE_C_COMPILER clang)
    set(CMAKE_CXX_COMPILER clang++)
endif()

project(
    pgvectorBench
    VERSION 0.1.0
    DESCRIPTION "pgvectorBench: a benchmark toolset for vector databases"
    HOMEPAGE_URL "https://github.com/zhjwpku/pgvectorBench"
    LANGUAGES CXX C)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
endif()

string(TOUPPER "${CMAKE_BUILD_TYPE}" uppercase_CMAKE_BUILD_TYPE)
if (uppercase_CMAKE_BUILD_TYPE STREQUAL "DEBUG")
    set(CMAKE_BUILD_TYPE "Debug")
endif()

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}") 
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
endif()

find_package(PG REQUIRED)
find_package(Parquet REQUIRED)
find_package(Threads REQUIRED)

add_subdirectory(third_party)
add_subdirectory(src)
