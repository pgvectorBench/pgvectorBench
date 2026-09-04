#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include <arrow/api.h>

#include "dataset/parquet_ground_truth.h"

namespace {

bool appendNeighbors(arrow::ListBuilder &lists,
                     const std::vector<int64_t> &values) {
  if (!lists.Append().ok()) {
    return false;
  }
  auto *builder =
      static_cast<arrow::Int64Builder *>(lists.value_builder());
  return builder->AppendValues(values).ok();
}

} // namespace

int main() {
  arrow::Int64Builder id_builder;
  if (!id_builder.AppendValues({0, 1, 2, 3}).ok()) {
    return 1;
  }

  auto value_builder = std::make_shared<arrow::Int64Builder>();
  arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
  if (!appendNeighbors(list_builder, {12, 10, 11}) ||
      !appendNeighbors(list_builder, {21, 20}) ||
      !appendNeighbors(list_builder, {33, 30, 32, 31}) ||
      !list_builder.Append().ok() || !value_builder->Append(40).ok() ||
      !value_builder->AppendNull().ok()) {
    return 1;
  }

  std::shared_ptr<arrow::Array> id_array_base;
  std::shared_ptr<arrow::Array> list_array_base;
  if (!id_builder.Finish(&id_array_base).ok() ||
      !list_builder.Finish(&list_array_base).ok()) {
    return 1;
  }

  auto ids = std::static_pointer_cast<arrow::Int64Array>(id_array_base);
  auto lists = std::static_pointer_cast<arrow::ListArray>(list_array_base);
  auto values =
      std::static_pointer_cast<arrow::Int64Array>(lists->values());

  std::vector<std::vector<int64_t>> ground_truths(
      4, std::vector<int64_t>(2));
  for (int64_t row = 0; row < 3; row++) {
    pgvectorbench::copyParquetGroundTruthRow(*ids, *lists, *values, row, 2,
                                             ground_truths);
  }

  if (ground_truths[0] != std::vector<int64_t>({10, 12}) ||
      ground_truths[1] != std::vector<int64_t>({20, 21}) ||
      ground_truths[2] != std::vector<int64_t>({30, 33})) {
    return 1;
  }

  try {
    pgvectorbench::copyParquetGroundTruthRow(*ids, *lists, *values, 3, 2,
                                             ground_truths);
  } catch (const std::runtime_error &) {
    return 0;
  }

  return 1;
}
