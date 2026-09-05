#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include "dataset/parquet_ground_truth.h"

namespace {

template <typename ListBuilderType>
bool appendNeighbors(ListBuilderType &lists,
                     const std::vector<int64_t> &values) {
  if (!lists.Append().ok()) {
    return false;
  }
  auto *builder =
      static_cast<arrow::Int64Builder *>(lists.value_builder());
  return builder->AppendValues(values).ok();
}

class ParquetGroundTruthTest : public testing::Test {
protected:
  void SetUp() override {
    arrow::Int64Builder id_builder;
    ASSERT_TRUE(id_builder.AppendValues({0, 1, 2, 3}).ok());

    auto value_builder = std::make_shared<arrow::Int64Builder>();
    arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
    ASSERT_TRUE(appendNeighbors(list_builder, {12, 10, 11}));
    ASSERT_TRUE(appendNeighbors(list_builder, {21, 20}));
    ASSERT_TRUE(appendNeighbors(list_builder, {33, 30, 32, 31}));
    ASSERT_TRUE(list_builder.Append().ok());
    ASSERT_TRUE(value_builder->Append(40).ok());
    ASSERT_TRUE(value_builder->AppendNull().ok());

    std::shared_ptr<arrow::Array> id_array_base;
    std::shared_ptr<arrow::Array> list_array_base;
    ASSERT_TRUE(id_builder.Finish(&id_array_base).ok());
    ASSERT_TRUE(list_builder.Finish(&list_array_base).ok());

    ids = std::static_pointer_cast<arrow::Int64Array>(id_array_base);
    lists = std::static_pointer_cast<arrow::ListArray>(list_array_base);
    values = std::static_pointer_cast<arrow::Int64Array>(lists->values());
  }

  std::shared_ptr<arrow::Int64Array> ids;
  std::shared_ptr<arrow::ListArray> lists;
  std::shared_ptr<arrow::Int64Array> values;
};

TEST_F(ParquetGroundTruthTest, CopiesNeighborsUsingListOffsets) {
  std::vector<std::vector<int64_t>> ground_truths(
      4, std::vector<int64_t>(2));
  for (int64_t row = 0; row < 3; row++) {
    pgvectorbench::copyParquetGroundTruthRow(*ids, *lists, *values, row, 2,
                                             ground_truths);
  }

  EXPECT_EQ(ground_truths[0], std::vector<int64_t>({10, 12}));
  EXPECT_EQ(ground_truths[1], std::vector<int64_t>({20, 21}));
  EXPECT_EQ(ground_truths[2], std::vector<int64_t>({30, 33}));
}

TEST_F(ParquetGroundTruthTest, RejectsNullNeighbors) {
  std::vector<std::vector<int64_t>> ground_truths(
      4, std::vector<int64_t>(2));
  EXPECT_THROW(pgvectorbench::copyParquetGroundTruthRow(
                   *ids, *lists, *values, 3, 2, ground_truths),
               std::runtime_error);
}

TEST_F(ParquetGroundTruthTest, SupportsLargeLists) {
  auto large_value_builder = std::make_shared<arrow::Int64Builder>();
  arrow::LargeListBuilder large_list_builder(arrow::default_memory_pool(),
                                              large_value_builder);
  ASSERT_TRUE(appendNeighbors(large_list_builder, {42, 40, 41}));

  std::shared_ptr<arrow::Array> large_list_array_base;
  ASSERT_TRUE(large_list_builder.Finish(&large_list_array_base).ok());
  auto large_lists =
      std::static_pointer_cast<arrow::LargeListArray>(large_list_array_base);
  auto large_values =
      std::static_pointer_cast<arrow::Int64Array>(large_lists->values());
  std::vector<std::vector<int64_t>> large_ground_truths(
      4, std::vector<int64_t>(2));
  pgvectorbench::copyParquetGroundTruthRow(*ids, *large_lists, *large_values,
                                           0, 2, large_ground_truths);
  EXPECT_EQ(large_ground_truths[0], std::vector<int64_t>({40, 42}));
}

} // namespace
