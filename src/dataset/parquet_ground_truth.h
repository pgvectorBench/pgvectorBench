#pragma once

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include <arrow/array.h>

namespace pgvectorbench {

template <typename ListArrayType>
inline void copyParquetGroundTruthRow(
    const arrow::Int64Array &ids, const ListArrayType &neighbors,
    const arrow::Int64Array &neighbor_values, int64_t row, size_t top_k,
    std::vector<std::vector<int64_t>> &ground_truths) {
  if (ids.IsNull(row) || neighbors.IsNull(row)) {
    throw std::runtime_error("ground-truth rows must not contain nulls");
  }

  const int64_t id = ids.Value(row);
  if (id < 0 || static_cast<size_t>(id) >= ground_truths.size()) {
    throw std::runtime_error("ground-truth row id is out of range");
  }

  const auto begin = neighbors.value_offset(row);
  const auto end = neighbors.value_offset(row + 1);
  if (static_cast<size_t>(end - begin) < top_k) {
    throw std::runtime_error("ground-truth row has too few neighbors");
  }

  for (size_t j = 0; j < top_k; j++) {
    const int64_t value_index = begin + static_cast<int64_t>(j);
    if (neighbor_values.IsNull(value_index)) {
      throw std::runtime_error("ground-truth neighbors must not contain nulls");
    }
    ground_truths[id][j] = neighbor_values.Value(value_index);
  }
  std::sort(ground_truths[id].begin(), ground_truths[id].end());
}

} // namespace pgvectorbench
