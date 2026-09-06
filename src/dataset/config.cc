#include "dataset.h"
#include "vector_config.h"

#include <filesystem>
#include <fstream>
#include <limits>
#include <regex>
#include <set>
#include <nlohmann/json.hpp>

namespace pgvectorbench {
DataSet loadDataSetConfig(const std::string &filename) {
  using Json = nlohmann::json;
  const auto file = std::filesystem::absolute(filename);
  std::ifstream stream(file);
  if (!stream) throw std::runtime_error("cannot open dataset config: " + filename);
  const auto j = Json::parse(stream);
  if (j.at("schema_version") != 1) throw std::runtime_error("unsupported dataset schema_version");
  auto positive = [](const Json &value) -> size_t {
    if (!value.is_number_unsigned() || value.get<uint64_t>() == 0 ||
        value.get<uint64_t>() > std::numeric_limits<size_t>::max())
      throw std::runtime_error("dataset counts and dimensions must be positive integers");
    return value.get<size_t>();
  };
  const auto name = j.at("name").get<std::string>();
  if (!std::regex_match(name, std::regex("[a-z_][a-z0-9_]{0,62}")))
    throw std::runtime_error("dataset name must be a lowercase SQL identifier (max 63 bytes)");
  const auto type = parseVectorType(j.at("vector_type").get<std::string>());
  const auto metric = parseMetric(j.at("metric").get<std::string>());
  const auto dim = positive(j.at("dimensions"));
  const auto format_name = j.value("format", std::string("parquet"));
  DataSetFormat format;
  if (format_name == "parquet") format = DataSetFormat::PARQUET_FORMAT;
  else if (format_name == "fvecs") format = DataSetFormat::FVECS_FORMAT;
  else if (format_name == "bvecs") format = DataSetFormat::BVECS_FORMAT;
  else throw std::runtime_error("unsupported dataset format");
  if (format != DataSetFormat::PARQUET_FORMAT && !denseType(type))
    throw std::runtime_error("bit and sparsevec datasets require Parquet");
  auto base_type = format == DataSetFormat::BVECS_FORMAT ? DataSetBaseType::BYTE : DataSetBaseType::FLOAT;
  if (j.value("scalar_type", std::string("float32")) == "float64") {
    if (format != DataSetFormat::PARQUET_FORMAT) throw std::runtime_error("float64 requires Parquet");
    base_type = DataSetBaseType::DOUBLE;
  } else if (j.value("scalar_type", std::string("float32")) != "float32") {
    throw std::runtime_error("scalar_type must be float32 or float64");
  }
  auto entry = [&](const Json &value) {
    const auto path = value.at("path").get<std::string>();
    const std::filesystem::path p(path);
    if (path.empty() || p.is_absolute()) throw std::runtime_error("dataset file paths must be relative");
    for (const auto &part : p) if (part == "..") throw std::runtime_error("dataset file paths cannot contain ..");
    return std::make_pair(path, positive(value.at("rows")));
  };
  std::vector<std::pair<std::string, size_t>> bases;
  std::set<std::string> paths;
  size_t total = 0;
  for (const auto &b : j.at("base_files")) {
    auto e = entry(b);
    if (!paths.insert(e.first).second || e.second > std::numeric_limits<size_t>::max() - total)
      throw std::runtime_error("duplicate base file or row count overflow");
    total += e.second;
    bases.push_back(e);
  }
  if (total == 0) throw std::runtime_error("base_files must not be empty");
  auto queries = entry(j.at("query_file"));
  auto gt = entry(j.at("ground_truth"));
  const auto k = positive(j.at("ground_truth").at("neighbors"));
  if (gt.second != queries.second || k > total) throw std::runtime_error("inconsistent dataset ground truth counts");
  auto root = std::filesystem::path(j.value("root", std::string(".")));
  if (root.is_relative()) root = file.parent_path() / root;
  DataSet ds(root.lexically_normal().string(), name, format, base_type, metric,
             bases, {{"id", "bigint"}, {"emb", sqlType(type, dim)}}, {}, "emb", dim, total, queries, gt, k);
  ds.source_type_ = type;
  configureVectors(ds, type, IndexRepresentation::NATIVE);
  return ds;
}
} // namespace pgvectorbench
