#pragma once

#include <sstream>
#include <cmath>
#include "vector_config.h"
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
      if (values->IsNull(j) || !std::isfinite(values->Value(j))) {
        throw std::runtime_error("Parquet embeddings must contain finite, non-null values");
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

// Converted datasets use id:int64 followed by emb. Sparse indices are zero-based
// in Parquet and converted to PostgreSQL's one-based syntax only at this boundary.
template <typename DataType>
std::string formatTypedEmbedding(const arrow::Array &column, int64_t row,
                                 const DataSet &ds) {
  if (denseType(ds.source_type_))
    return formatParquetEmbedding<DataType>(column, row, ds.dim_);
  if (ds.source_type_ == VectorType::BIT) {
    if (column.type_id() != arrow::Type::STRING || column.IsNull(row))
      throw std::runtime_error("bit embeddings must be non-null strings");
    auto bits = static_cast<const arrow::StringArray &>(column).GetString(row);
    if (bits.size() != ds.dim_ || bits.find_first_not_of("01") != std::string::npos)
      throw std::runtime_error("bit embedding has invalid length or characters");
    return bits;
  }
  if (column.type_id() != arrow::Type::STRUCT || column.IsNull(row))
    throw std::runtime_error("sparse embedding must be a non-null struct of indices and values");
  const auto &record = static_cast<const arrow::StructArray &>(column);
  const auto indices = record.GetFieldByName("indices");
  const auto weights = record.GetFieldByName("values");
  if (!indices || !weights || indices->type_id() != arrow::Type::LIST ||
      weights->type_id() != arrow::Type::LIST || indices->IsNull(row) || weights->IsNull(row))
    throw std::runtime_error("sparse indices and values must be non-null lists");
  const auto &il = static_cast<const arrow::ListArray &>(*indices);
  const auto &vl = static_cast<const arrow::ListArray &>(*weights);
  using Values = std::conditional_t<std::is_same_v<DataType, float>, arrow::FloatArray, arrow::DoubleArray>;
  auto ia = std::dynamic_pointer_cast<arrow::Int32Array>(il.values());
  auto va = std::dynamic_pointer_cast<Values>(vl.values());
  const auto length = il.value_length(row);
  if (!ia || !va || length != vl.value_length(row) || length > 16000)
    throw std::runtime_error("invalid sparse value types, lengths or more than 16000 nonzero elements");
  std::ostringstream out;
  out << '{';
  int32_t previous = -1;
  char number[40];
  for (int64_t n = 0; n < length; ++n) {
    const auto i = il.value_offset(row) + n;
    const auto v = vl.value_offset(row) + n;
    const auto index = ia->Value(i);
    const auto value = va->Value(v);
    if (ia->IsNull(i) || va->IsNull(v) || index <= previous || index < 0 ||
        static_cast<size_t>(index) >= ds.dim_ || !std::isfinite(value) || value == 0)
      throw std::runtime_error("sparse entries require sorted unique in-range indices and finite nonzero values");
    if (n) out << ',';
    if constexpr (std::is_same_v<DataType, float>) f2s_buffered(value, number);
    else d2s_buffered(value, number);
    out << index + 1 << ':' << number;
    previous = index;
  }
  out << "}/" << ds.dim_;
  return out.str();
}

} // namespace pgvectorbench
