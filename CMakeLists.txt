cmake_minimum_required(VERSION 3.25)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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

include(CTest)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
endif()

string(TOUPPER "${CMAKE_BUILD_TYPE}" uppercase_CMAKE_BUILD_TYPE)
if (uppercase_CMAKE_BUILD_TYPE STREQUAL "DEBUG")
    set(CMAKE_BUILD_TYPE "Debug")
endif()

option(PGVECTORBENCH_USE_COMPILER_CACHE
       "Use sccache or ccache when available" ON)
if(PGVECTORBENCH_USE_COMPILER_CACHE AND
   (NOT CMAKE_C_COMPILER_LAUNCHER OR NOT CMAKE_CXX_COMPILER_LAUNCHER))
    find_program(PGVECTORBENCH_COMPILER_CACHE NAMES sccache ccache)
    if(PGVECTORBENCH_COMPILER_CACHE)
        if(NOT CMAKE_C_COMPILER_LAUNCHER)
            set(CMAKE_C_COMPILER_LAUNCHER "${PGVECTORBENCH_COMPILER_CACHE}")
        endif()
        if(NOT CMAKE_CXX_COMPILER_LAUNCHER)
            set(CMAKE_CXX_COMPILER_LAUNCHER "${PGVECTORBENCH_COMPILER_CACHE}")
        endif()
        message(STATUS
                "Using compiler cache: ${PGVECTORBENCH_COMPILER_CACHE}")
    endif()
endif()

find_package(PG REQUIRED)
find_package(Threads REQUIRED)

include(ThirdParty)
add_subdirectory(src)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
