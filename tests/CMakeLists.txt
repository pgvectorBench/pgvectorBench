add_executable(client_factory_test client_factory_test.cc)

target_include_directories(
  client_factory_test
  PRIVATE ${PG_INCLUDE_DIRS}
  PRIVATE ${CMAKE_SOURCE_DIR}/src
)

target_link_directories(client_factory_test PRIVATE ${PG_LIBRARY_DIRS})

target_link_libraries(
  client_factory_test
  PRIVATE spdlog::spdlog
  PRIVATE ${PG_LIBRARIES}
)

add_test(NAME client_factory_test COMMAND client_factory_test)
set_tests_properties(client_factory_test PROPERTIES SKIP_RETURN_CODE 77)

add_test(NAME cli_version COMMAND pgvectorbench --version)
add_test(NAME cli_help COMMAND pgvectorbench --help)

add_executable(parquet_ground_truth_test parquet_ground_truth_test.cc)

target_include_directories(
  parquet_ground_truth_test
  PRIVATE ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(
  parquet_ground_truth_test
  PRIVATE pgvectorbench::arrow
)

add_test(NAME parquet_ground_truth_test COMMAND parquet_ground_truth_test)
