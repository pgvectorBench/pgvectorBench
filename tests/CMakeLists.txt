include(GoogleTest)

add_executable(client_factory_test client_factory_test.cc)

target_include_directories(
  client_factory_test
  PRIVATE ${PG_INCLUDE_DIRS}
  PRIVATE ${CMAKE_SOURCE_DIR}/src
)

target_link_directories(client_factory_test PRIVATE ${PG_LIBRARY_DIRS})

target_link_libraries(
  client_factory_test
  PRIVATE GTest::gtest_main
  PRIVATE spdlog::spdlog
  PRIVATE ${PG_LIBRARIES}
)

gtest_discover_tests(client_factory_test)

add_test(NAME cli_version COMMAND pgvectorbench --version)
add_test(NAME cli_help COMMAND pgvectorbench --help)

add_executable(parquet_ground_truth_test parquet_ground_truth_test.cc)

target_include_directories(
  parquet_ground_truth_test
  PRIVATE ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(
  parquet_ground_truth_test
  PRIVATE GTest::gtest_main
  PRIVATE pgvectorbench::arrow
)

gtest_discover_tests(parquet_ground_truth_test)

add_executable(parquet_zstd_test parquet_zstd_test.cc)

target_link_libraries(
  parquet_zstd_test
  PRIVATE GTest::gtest_main
  PRIVATE pgvectorbench::parquet
)

gtest_discover_tests(parquet_zstd_test)
