#pragma once

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string>

#include "dataset.h"

namespace pgvectorbench {

inline std::string typeName(VectorType type) {
  switch (type) {
  case VectorType::VECTOR: return "vector";
  case VectorType::HALFVEC: return "halfvec";
  case VectorType::BIT: return "bit";
  case VectorType::SPARSEVEC: return "sparsevec";
  }
  throw std::runtime_error("invalid vector type");
}

inline VectorType parseVectorType(const std::string &type) {
  for (auto t : {VectorType::VECTOR, VectorType::HALFVEC, VectorType::BIT,
                 VectorType::SPARSEVEC}) {
    if (typeName(t) == type) return t;
  }
  throw std::runtime_error("unsupported vector type: " + type);
}

inline std::string representationName(IndexRepresentation r) {
  switch (r) {
  case IndexRepresentation::NATIVE: return "native";
  case IndexRepresentation::HALFVEC: return "halfvec";
  case IndexRepresentation::BINARY: return "binary";
  }
  throw std::runtime_error("invalid index representation");
}

inline IndexRepresentation parseRepresentation(const std::string &name) {
  for (auto r : {IndexRepresentation::NATIVE, IndexRepresentation::HALFVEC,
                 IndexRepresentation::BINARY}) {
    if (representationName(r) == name) return r;
  }
  throw std::runtime_error("unsupported index representation: " + name);
}

inline std::string distanceName(DataSetMetric m) {
  switch (m) {
  case DataSetMetric::L1: return "l1";
  case DataSetMetric::L2: return "l2";
  case DataSetMetric::IP: return "ip";
  case DataSetMetric::COSINE: return "cosine";
  case DataSetMetric::HAMMING: return "hamming";
  case DataSetMetric::JACCARD: return "jaccard";
  }
  throw std::runtime_error("invalid metric");
}

inline DataSetMetric parseMetric(const std::string &name) {
  for (auto m : {DataSetMetric::L1, DataSetMetric::L2, DataSetMetric::IP,
                 DataSetMetric::COSINE, DataSetMetric::HAMMING, DataSetMetric::JACCARD}) {
    if (distanceName(m) == name) return m;
  }
  throw std::runtime_error("unsupported metric: " + name);
}

inline bool denseType(VectorType t) {
  return t == VectorType::VECTOR || t == VectorType::HALFVEC;
}

inline VectorType indexVectorType(const DataSet &ds) {
  if (ds.representation_ == IndexRepresentation::BINARY) return VectorType::BIT;
  if (ds.representation_ == IndexRepresentation::HALFVEC) return VectorType::HALFVEC;
  return ds.storage_type_;
}

inline DataSetMetric searchMetric(const DataSet &ds) {
  return ds.representation_ == IndexRepresentation::BINARY
      ? DataSetMetric::HAMMING : ds.metric_;
}

inline std::string sqlType(VectorType type, size_t dim) {
  return typeName(type) + "(" + std::to_string(dim) + ")";
}

inline void validateVectorConfig(const DataSet &ds) {
  const bool bits = ds.source_type_ == VectorType::BIT;
  if (bits != (ds.metric_ == DataSetMetric::HAMMING || ds.metric_ == DataSetMetric::JACCARD))
    throw std::runtime_error("source vector type and evaluation metric are incompatible");
  if (ds.storage_type_ != ds.source_type_ &&
      !(denseType(ds.storage_type_) && denseType(ds.source_type_)))
    throw std::runtime_error("storage conversion is supported only between vector and halfvec");
  if (ds.representation_ != IndexRepresentation::NATIVE && !denseType(ds.storage_type_))
    throw std::runtime_error("expression indexes require vector or halfvec storage");
  if (ds.representation_ == IndexRepresentation::HALFVEC && ds.storage_type_ != VectorType::VECTOR)
    throw std::runtime_error("halfvec representation requires vector storage; use native for halfvec storage");
  const size_t max_dim = denseType(ds.storage_type_) ? 16000 :
      ds.storage_type_ == VectorType::SPARSEVEC ? 1000000000 : 64000;
  if (ds.dim_ == 0 || ds.dim_ > max_dim)
    throw std::runtime_error("dimension exceeds supported storage limit");
}

inline void configureVectors(DataSet &ds, VectorType storage, IndexRepresentation representation) {
  ds.storage_type_ = storage;
  ds.representation_ = representation;
  validateVectorConfig(ds);
  for (auto &[name, type] : ds.fields_) {
    if (name == ds.vector_field_) type = sqlType(storage, ds.dim_);
  }
}

inline std::string indexOpclass(const DataSet &ds) {
  return typeName(indexVectorType(ds)) + "_" + distanceName(searchMetric(ds)) + "_ops";
}

inline void validateIndexConfig(const DataSet &ds, const std::string &method) {
  validateVectorConfig(ds);
  if (method != "hnsw" && method != "ivfflat")
    throw std::runtime_error("unsupported index type: " + method);
  const auto t = indexVectorType(ds);
  const auto m = searchMetric(ds);
  if (method == "ivfflat" && (t == VectorType::SPARSEVEC || m == DataSetMetric::L1 || m == DataSetMetric::JACCARD))
    throw std::runtime_error("ivfflat does not support " + typeName(t) + "/" + distanceName(m));
  const size_t max_dim = t == VectorType::VECTOR ? 2000 : t == VectorType::HALFVEC ? 4000 :
      t == VectorType::BIT ? 64000 : 1000000000;
  if (ds.dim_ > max_dim)
    throw std::runtime_error("dimension exceeds " + method + " " + typeName(t) + " index limit");
}

inline std::string quoteIdentifier(const std::string &name) {
  std::string result = "\"";
  for (char c : name) {
    if (c == '"') result += '"';
    result += c;
  }
  return result + '"';
}

inline std::string indexExpression(const DataSet &ds, const std::string &operand) {
  if (ds.representation_ == IndexRepresentation::BINARY)
    return "(binary_quantize(" + operand + ")::bit(" + std::to_string(ds.dim_) + "))";
  if (ds.representation_ == IndexRepresentation::HALFVEC)
    return "(" + operand + "::halfvec(" + std::to_string(ds.dim_) + "))";
  return operand;
}

inline std::string searchSql(const DataSet &ds, const std::string &table,
                             const std::string &embedding, size_t k,
                             bool rerank = false, size_t candidates = 0) {
  const auto column = quoteIdentifier(ds.vector_field_);
  // Embeddings are produced by the validated numeric/bit formatters, never raw SQL.
  const auto literal = "'" + embedding + "'" +
      (ds.representation_ == IndexRepresentation::NATIVE ? "" : "::" + sqlType(ds.storage_type_, ds.dim_));
  std::string filter;
  for (const auto &f : ds.filter_fields_)
    filter += std::get<0>(f) + std::get<1>(f) + std::get<2>(f) + std::get<3>(f) + std::get<4>(f);
  const auto from = " FROM " + table + (filter.empty() ? "" : " WHERE " + filter);
  const auto order = " ORDER BY " + indexExpression(ds, column) + " " +
      std::string(metric2operator(searchMetric(ds))) + " " + indexExpression(ds, literal);
  if (!rerank) return "SELECT id" + from + order + " LIMIT " + std::to_string(k) + ";";
  if (ds.storage_type_ != VectorType::VECTOR || ds.representation_ == IndexRepresentation::NATIVE || candidates < k)
    throw std::runtime_error("rerank requires vector storage, an expression index and candidate_k >= k2");
  return "WITH candidates AS MATERIALIZED (SELECT id, " + column + from + order +
      " LIMIT " + std::to_string(candidates) + ") SELECT id FROM candidates ORDER BY " +
      column + " " + std::string(metric2operator(ds.metric_)) + " " + literal +
      " LIMIT " + std::to_string(k) + ";";
}

inline const std::vector<std::string> querySettingNames = {
  "hnsw.ef_search", "hnsw.iterative_scan", "hnsw.max_scan_tuples", "hnsw.scan_mem_multiplier",
  "ivfflat.probes", "ivfflat.iterative_scan", "ivfflat.max_probes"};

} // namespace pgvectorbench
