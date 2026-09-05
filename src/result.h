#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace pgvectorbench {

struct ColumnResult {
  std::string name;
  std::string type;
  std::string type_name;
  int dimensions = -1;
};

struct IndexMetadata {
  std::string schema;
  std::string name;
  std::string type;
  uint64_t size_bytes = 0;
  bool valid = false;
  bool ready = false;
  std::vector<std::string> columns;
  std::vector<std::string> operator_classes;
  std::map<std::string, std::string> options;
  std::optional<std::string> predicate;
};

struct TableResult {
  std::string schema;
  std::string name;
  std::optional<double> estimated_rows;
  uint64_t table_size_bytes = 0; // Includes TOAST and auxiliary forks, not indexes.
  uint64_t indexes_size_bytes = 0;
  uint64_t total_size_bytes = 0;
  std::vector<ColumnResult> columns;
  std::vector<IndexMetadata> indexes;
  std::vector<std::string> warnings;
};

struct EnvironmentResult {
  std::string database;
  std::string postgres_version;
  std::optional<std::string> pgvector_version;
  std::map<std::string, std::string> server_settings;
};

struct PhaseResult {
  std::string table_name;
  double elapsed_seconds = 0;
  std::optional<TableResult> table;
};

struct LoadResult : PhaseResult {
  uint64_t rows = 0;
  uint64_t copy_bytes = 0;
  double rows_per_second = 0;
  double copy_bytes_per_second = 0;
};

struct IndexResult : PhaseResult {
  std::string index_name;
  std::string index_type;
  std::map<std::string, std::string> effective_settings;
};

struct SetupResult : PhaseResult {
  std::optional<IndexResult> index;
};

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
  // Explicitly applied SET values, alongside the effective session settings.
  std::map<std::string, std::string> session_overrides;
};

struct QueryResult {
  DatasetResult dataset;
  QueryConfig config;
  double elapsed_seconds = 0;
  double qps = 0;
  DistributionResult latency_us;
  DistributionResult recall;
  std::map<std::string, std::string> effective_settings;
  std::optional<TableResult> table;
};

// Only complete, successful runs produce a BenchmarkResult. Failures use the
// CLI's nonzero exit status and never serialize partial query statistics.
struct BenchmarkResult {
  std::string tool_version;
  std::optional<QueryResult> query;
  std::optional<EnvironmentResult> environment;
  std::optional<SetupResult> setup;
  std::optional<LoadResult> load;
  std::optional<IndexResult> index;
  std::optional<PhaseResult> teardown;
};

void logQueryResult(const QueryResult &result);
std::string resultToJson(const BenchmarkResult &result);

} // namespace pgvectorbench
