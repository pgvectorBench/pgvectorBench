#pragma once

#include <sstream>
#include <stdexcept>
#include <type_traits>

#include <arrow/api.h>
#include <ryu/ryu.h>

namespace pgvectorbench {

inline const arrow::Int64Array &parquetRowIds(const arrow::RecordBatch &batch) {
  if (batch.num_columns() < 2 ||
      batch.column(0)->type_id() != arrow::Type::INT64) {
    throw std::runtime_error(
        "Parquet rows require int64 ids and a list column");
  }
  return static_cast<const arrow::Int64Array &>(*batch.column(0));
}

// Both COPY and query SQL must respect the offsets of sliced/variable lists.
template <typename DataType>
std::string formatParquetEmbedding(const arrow::Array &column, int64_t row,
                                   size_t dimension) {
  using ArrayType = std::conditional_t<std::is_same_v<DataType, float>,
                                       arrow::FloatArray, arrow::DoubleArray>;
  auto format = [&](const auto &lists) {
    auto values = std::dynamic_pointer_cast<ArrayType>(lists.values());
    if (!values || lists.IsNull(row) ||
        lists.value_length(row) != static_cast<int64_t>(dimension)) {
      throw std::runtime_error(
          "invalid Parquet embedding type, null or dimension");
    }
    const auto begin = lists.value_offset(row);
    const auto end = lists.value_offset(row + 1);
    std::ostringstream out;
    out << '[';
    char result[40];
    for (auto j = begin; j < end; ++j) {
      if (values->IsNull(j)) {
        throw std::runtime_error("Parquet embeddings must not contain nulls");
      }
      if (j != begin) {
        out << ',';
      }
      if constexpr (std::is_same_v<DataType, float>) {
        f2s_buffered(values->Value(j), result);
      } else {
        d2s_buffered(values->Value(j), result);
      }
      out << result;
    }
    out << ']';
    return out.str();
  };
  if (column.type_id() == arrow::Type::LIST) {
    return format(static_cast<const arrow::ListArray &>(column));
  }
  if (column.type_id() == arrow::Type::LARGE_LIST) {
    return format(static_cast<const arrow::LargeListArray &>(column));
  }
  throw std::runtime_error("Parquet embeddings must be a list or large_list");
}

} // namespace pgvectorbench
