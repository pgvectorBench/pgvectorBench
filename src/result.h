#pragma once

#include <cstddef>
#include <map>
#include <string>
#include <vector>

namespace pgvectorbench {

struct PercentileResult {
  std::string label; // Preserve the spelling used in human-readable output.
  double percentage;
  double value;
};

struct DistributionResult {
  size_t count = 0;
  double best = 0;
  double worst = 0;
  double average = 0;
  std::vector<PercentileResult> percentiles;
};

struct DatasetResult {
  std::string name;
  std::string format;
  std::string metric;
  size_t dimensions = 0;
  size_t base_vectors = 0; // Dataset metadata, not a database row count.
  size_t query_vectors = 0;
  size_t ground_truth_neighbors = 0;
};

struct QueryConfig {
  std::string table_name;
  std::string vector_column;
  size_t k1 = 0;
  size_t k2 = 0;
  size_t thread_num = 0;
  size_t loop = 0;
  // Explicitly applied SET values only. Unspecified server defaults are not
  // inferred; environment introspection can supply those in a later version.
  std::map<std::string, std::string> session_overrides;
};

struct QueryResult {
  DatasetResult dataset;
  QueryConfig config;
  double elapsed_seconds = 0;
  double qps = 0;
  DistributionResult latency_us;
  DistributionResult recall;
};

// Only complete, successful runs produce a BenchmarkResult. Failures use the
// CLI's nonzero exit status and never serialize partial query statistics.
struct BenchmarkResult {
  std::string tool_version;
  QueryResult query;
};

void logQueryResult(const QueryResult &result);
std::string resultToJson(const BenchmarkResult &result);

} // namespace pgvectorbench
