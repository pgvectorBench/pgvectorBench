add_executable(
  pgvectorbench
  pgvectorbench.cc
  dataset/dataset.cc
  index.cc
  setup.cc
  load.cc
  query.cc
  teardown.cc
)

if(NOT APPLE AND CMAKE_BUILD_TYPE MATCHES "Debug")
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
      target_compile_options(pgvectorbench PRIVATE -fsanitize=address)
      target_link_options(pgvectorbench PRIVATE -fsanitize=address)
  endif()
endif()

set_target_properties(
  pgvectorbench PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
)

target_include_directories(
  pgvectorbench
  PRIVATE ${PG_INCLUDE_DIRS}
  PRIVATE ${PARQUET_INCLUDE_DIR}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_directories(
  pgvectorbench
  PRIVATE ${PG_LIBRARY_DIRS}
)

target_link_libraries(
  pgvectorbench
  PRIVATE argparse::argparse
  PRIVATE concurrentqueue
  PRIVATE ryu::ryu
  PRIVATE spdlog::spdlog
  PRIVATE ${PG_LIBRARIES}
  PRIVATE Parquet::parquet_shared
  PRIVATE Threads::Threads
)

install(TARGETS pgvectorbench RUNTIME)
