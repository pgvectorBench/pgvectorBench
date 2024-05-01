#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <spdlog/fmt/ostr.h>

namespace pgvectorbench {

enum class DataSetFormat : uint8_t {
  FVECS_FORMAT, // float vector
  BVECS_FORMAT, // byte vector
  PARQUET_FORMAT,
};

enum class DataSetBaseType : uint8_t {
  BYTE,
  INT,
  FLOAT,
  DOUBLE,
};

// Ground truth of dataset are calculated using one of the following metric
enum class DataSetMetric : uint8_t {
  L1,     // Manhattan distance
  L2,     // Euclidean distance
  IP,     // Inner product
  COSINE, // Cosine similarity

  // for binary vectors
  HAMMING, // Hanmming distance
  JACCARD, // Jaccard distance
};

std::string_view metric2ops(enum DataSetMetric metric);
std::string_view metric2operator(enum DataSetMetric metric);

struct DataSet {
  DataSet(std::string location, std::string name, DataSetFormat format,
          DataSetBaseType base_type, DataSetMetric metric,
          std::vector<std::pair<std::string, size_t>> base_files,
          std::vector<std::pair<std::string, std::string>> fields,
          std::string vector_field, size_t dim, size_t total_cnt,
          std::pair<std::string, size_t> query_file,
          std::pair<std::string, size_t> gt_file, size_t gt_topk)
      : location_(location), name_(name), format_(format),
        base_type_(base_type), metric_(metric), base_files_(base_files),
        fields_(fields), vector_field_(vector_field), dim_(dim),
        total_cnt_(total_cnt), query_file_(query_file), gt_file_(gt_file),
        gt_topk_(gt_topk) {
    validate();
  }

  void set_location(std::string location) { location_ = location; }

  void validate() {
    size_t sum = 0;
    for (auto file : base_files_) {
      sum += file.second;
    }
    assert(sum == total_cnt_);
  }

  std::string location_;      // directory of the dataset
  std::string name_;          // name of the dataset, usually used as table name
  DataSetFormat format_;      // type of the dataset
  DataSetBaseType base_type_; // base type of the data element
  DataSetMetric metric_;      // metric used for ground truth generation

  // base set files
  std::vector<std::pair<std::string, size_t>> base_files_;
  std::vector<std::pair<std::string, std::string>>
      fields_;               // field name, type pair
  std::string vector_field_; // build vector index on this field
  size_t dim_;               // dimention of the vector field
  size_t total_cnt_;         // nb base vectors

  // query set file, filename/query_cnt pair
  std::pair<std::string, size_t> query_file_;

  // groundtruth file, filename/result_cnt pair
  std::pair<std::string, size_t> gt_file_;
  size_t gt_topk_; // topk groudtruth results for each query
};

std::ostream &operator<<(std::ostream &os, const DataSet &dataset);

DataSet *getDataSet(const std::string &ds_name);

struct VecsBlock {
  VecsBlock(const char *buffer, size_t start_id, size_t batch_size,
            const DataSet *dataset)
      : buffer_(buffer), start_id_(start_id), batch_size_(batch_size),
        dataset_(dataset) {}

  const char *buffer_;
  size_t start_id_;   // We have to generate item id according to its real
                      // position in VECS file, this is the start id for the
                      // specific data block
  size_t batch_size_; // number of row in this block
  const DataSet *dataset_;
};

} // namespace pgvectorbench
