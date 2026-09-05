#include "result.h"

#include <cmath>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

namespace pgvectorbench {
namespace {

template <typename Sample>
std::string distributionToString(const DistributionResult &result) {
  std::ostringstream out;
  out << "best=" << static_cast<Sample>(result.best)
      << " worst=" << static_cast<Sample>(result.worst)
      << " average=" << result.average;
  for (const auto &p : result.percentiles) {
    out << " P(" << p.label << "%)=" << static_cast<Sample>(p.value);
  }
  return out.str();
}

double finiteMetric(double value) {
  // nlohmann/json otherwise serializes NaN and infinity as null.
  if (!std::isfinite(value)) {
    throw std::runtime_error("cannot serialize a non-finite benchmark metric");
  }
  return value;
}

nlohmann::json distributionToJson(const DistributionResult &result) {
  auto percentiles = nlohmann::json::array();
  for (const auto &p : result.percentiles) {
    percentiles.push_back({{"percentage", finiteMetric(p.percentage)},
                            {"value", finiteMetric(p.value)}});
  }
  return {{"count", result.count},
          {"best", finiteMetric(result.best)},
          {"worst", finiteMetric(result.worst)},
          {"average", finiteMetric(result.average)},
          {"percentiles", std::move(percentiles)}};
}

} // namespace

void logQueryResult(const QueryResult &result) {
  SPDLOG_INFO("qps: {}", result.qps);
  SPDLOG_INFO("latency(us): {}", distributionToString<uint32_t>(result.latency_us));
  SPDLOG_INFO("recall: {}", distributionToString<double>(result.recall));
}

namespace {
using Json = nlohmann::json;

Json tableToJson(const std::optional<TableResult> &table) {
  if (!table) return nullptr;
  auto columns = Json::array();
  for (const auto &c : table->columns) {
    columns.push_back({{"name", c.name}, {"type", c.type},
        {"dimensions", c.type_name == "vector" && c.dimensions >= 0
                           ? Json(c.dimensions) : Json(nullptr)}});
  }
  auto indexes = Json::array();
  for (const auto &i : table->indexes) {
    indexes.push_back({{"schema", i.schema}, {"name", i.name}, {"type", i.type},
        {"size_bytes", i.size_bytes}, {"valid", i.valid}, {"ready", i.ready},
        {"columns", i.columns}, {"operator_classes", i.operator_classes},
        {"options", i.options}, {"predicate", i.predicate ? Json(*i.predicate) : Json(nullptr)}});
  }
  return {{"schema", table->schema}, {"name", table->name},
          {"estimated_rows", table->estimated_rows ? Json(finiteMetric(*table->estimated_rows)) : Json(nullptr)},
          {"table_size_bytes", table->table_size_bytes},
          {"indexes_size_bytes", table->indexes_size_bytes},
          {"total_size_bytes", table->total_size_bytes},
          {"columns", std::move(columns)}, {"indexes", std::move(indexes)},
          {"warnings", table->warnings}};
}

Json phaseToJson(const PhaseResult &phase) {
  return {{"table_name", phase.table_name},
          {"elapsed_seconds", finiteMetric(phase.elapsed_seconds)},
          {"table", tableToJson(phase.table)}};
}

Json indexToJson(const IndexResult &index) {
  auto output = phaseToJson(index);
  output["index_name"] = index.index_name;
  output["index_type"] = index.index_type;
  output["effective_settings"] = index.effective_settings;
  return output;
}

Json queryToJson(const QueryResult &q) {
  const auto &ds = q.dataset;
  const auto &cfg = q.config;
  return {{"dataset",
           {{"name", ds.name}, {"format", ds.format}, {"metric", ds.metric},
            {"dimensions", ds.dimensions}, {"base_vectors", ds.base_vectors},
            {"query_vectors", ds.query_vectors},
            {"ground_truth_neighbors", ds.ground_truth_neighbors}}},
          {"config",
           {{"table_name", cfg.table_name}, {"vector_column", cfg.vector_column},
            {"k1", cfg.k1}, {"k2", cfg.k2}, {"thread_num", cfg.thread_num},
            {"loop", cfg.loop}, {"session_overrides", cfg.session_overrides}}},
          {"elapsed_seconds", finiteMetric(q.elapsed_seconds)},
          {"qps", finiteMetric(q.qps)},
          {"latency_us", distributionToJson(q.latency_us)},
          {"recall", distributionToJson(q.recall)},
          {"effective_settings", q.effective_settings},
          {"table", tableToJson(q.table)}};
}
} // namespace

std::string resultToJson(const BenchmarkResult &result) {
  Json output = {{"schema_version", 2}, {"tool_version", result.tool_version},
                 {"status", "success"}, {"environment", nullptr},
                 {"setup", nullptr}, {"load", nullptr}, {"index", nullptr},
                 {"query", nullptr}, {"teardown", nullptr}};
  if (result.environment) {
    const auto &env = *result.environment;
    output["environment"] = {{"database", env.database},
        {"postgres_version", env.postgres_version},
        {"pgvector_version", env.pgvector_version ? Json(*env.pgvector_version) : Json(nullptr)},
        {"server_settings", env.server_settings}};
  }
  if (result.setup) {
    output["setup"] = phaseToJson(*result.setup);
    output["setup"]["index"] = result.setup->index ? indexToJson(*result.setup->index) : Json(nullptr);
  }
  if (result.load) {
    const auto &load = *result.load;
    output["load"] = phaseToJson(load);
    output["load"]["rows"] = load.rows;
    output["load"]["copy_bytes"] = load.copy_bytes;
    output["load"]["rows_per_second"] = finiteMetric(load.rows_per_second);
    output["load"]["copy_bytes_per_second"] = finiteMetric(load.copy_bytes_per_second);
  }
  if (result.index) output["index"] = indexToJson(*result.index);
  if (result.query) output["query"] = queryToJson(*result.query);
  if (result.teardown) output["teardown"] = phaseToJson(*result.teardown);
  // Serialize completely before the caller writes stdout.
  return output.dump() + "\n";
}

} // namespace pgvectorbench
