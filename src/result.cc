#include "result.h"

#include <cmath>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

namespace pgvectorbench {
namespace {

std::string distributionToString(const DistributionResult &result) {
  std::ostringstream out;
  out << "best=" << result.best << " worst=" << result.worst
      << " average=" << result.average;
  for (const auto &p : result.percentiles) {
    out << " P(" << p.label << "%)=" << p.value;
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
  SPDLOG_INFO("latency(us): {}", distributionToString(result.latency_us));
  SPDLOG_INFO("recall: {}", distributionToString(result.recall));
}

std::string resultToJson(const BenchmarkResult &result) {
  const auto &q = result.query;
  const auto &ds = q.dataset;
  const auto &cfg = q.config;
  const nlohmann::json output = {
      {"schema_version", 1},
      {"tool_version", result.tool_version},
      {"status", "success"},
      {"query",
       {{"dataset",
         {{"name", ds.name},
          {"format", ds.format},
          {"metric", ds.metric},
          {"dimensions", ds.dimensions},
          {"base_vectors", ds.base_vectors},
          {"query_vectors", ds.query_vectors},
          {"ground_truth_neighbors", ds.ground_truth_neighbors}}},
        {"config",
         {{"table_name", cfg.table_name},
          {"vector_column", cfg.vector_column},
          {"k1", cfg.k1},
          {"k2", cfg.k2},
          {"thread_num", cfg.thread_num},
          {"loop", cfg.loop},
          {"session_overrides", cfg.session_overrides}}},
        {"elapsed_seconds", finiteMetric(q.elapsed_seconds)},
        {"qps", finiteMetric(q.qps)},
        {"latency_us", distributionToJson(q.latency_us)},
        {"recall", distributionToJson(q.recall)}}}};
  // Serialize completely before the caller writes stdout.
  return output.dump() + "\n";
}

} // namespace pgvectorbench
