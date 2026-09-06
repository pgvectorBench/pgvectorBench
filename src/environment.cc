#include "environment.h"
#include "dataset/vector_config.h"

#include <algorithm>
#include <stdexcept>

#include <nlohmann/json.hpp>

namespace pgvectorbench {
namespace {

std::unique_ptr<Client> connect(const ClientFactory &factory) {
  auto client = factory.createClient();
  if (!client) {
    throw std::runtime_error("failed to connect for database inspection");
  }
  return client;
}

std::string value(PGresult *result, int row, int column) {
  return PQgetvalue(result, row, column);
}

} // namespace

std::map<std::string, std::string> readServerSettings(Client &client) {
  // Loading the vector type initializes pgvector's GUCs on a fresh connection.
  // The type identifier is quoted by PostgreSQL, not interpolated user input.
  auto extension = client.queryParams(
      "SELECT format('%I.vector', n.nspname) FROM pg_extension e "
      "JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='vector'");
  if (PQntuples(extension.get()) != 0) {
    client.queryParams("SELECT '[0]'::" + value(extension.get(), 0, 0));
  }
  auto result = client.queryParams(R"SQL(
    SELECT name, current_setting(name) FROM pg_settings
    WHERE name IN ('shared_buffers', 'effective_cache_size', 'work_mem',
      'maintenance_work_mem', 'max_parallel_workers',
      'max_parallel_workers_per_gather', 'max_parallel_maintenance_workers',
      'enable_seqscan', 'enable_indexscan', 'enable_indexonlyscan',
      'enable_bitmapscan', 'hnsw.ef_search', 'hnsw.iterative_scan',
      'hnsw.max_scan_tuples', 'hnsw.scan_mem_multiplier',
      'ivfflat.probes', 'ivfflat.iterative_scan', 'ivfflat.max_probes')
    ORDER BY name
  )SQL");
  std::map<std::string, std::string> settings;
  for (int row = 0; row < PQntuples(result.get()); ++row) {
    settings.emplace(value(result.get(), row, 0), value(result.get(), row, 1));
  }
  return settings;
}

EnvironmentResult inspectEnvironment(const ClientFactory &factory) {
  auto client = connect(factory);
  auto result = client->queryParams(
      "SELECT current_database(), current_setting('server_version'), "
      "(SELECT extversion FROM pg_extension WHERE extname='vector')");
  EnvironmentResult env;
  env.database = value(result.get(), 0, 0);
  env.postgres_version = value(result.get(), 0, 1);
  if (!PQgetisnull(result.get(), 0, 2)) {
    env.pgvector_version = value(result.get(), 0, 2);
  }
  env.server_settings = readServerSettings(*client);
  return env;
}

TableResult inspectTable(const ClientFactory &factory, const std::string &table) {
  auto client = connect(factory);
  auto relation = client->queryParams(R"SQL(
    SELECT c.oid, n.nspname, c.relname, c.reltuples,
      pg_table_size(c.oid), pg_indexes_size(c.oid), pg_total_relation_size(c.oid)
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.oid=to_regclass($1) AND c.relkind IN ('r', 'p', 'm')
  )SQL", {table});
  if (PQntuples(relation.get()) != 1) {
    auto database = client->queryParams("SELECT current_database()");
    throw std::runtime_error("table " + table + " not found or unsupported in database " +
                             value(database.get(), 0, 0));
  }
  TableResult output;
  const auto oid = value(relation.get(), 0, 0);
  output.schema = value(relation.get(), 0, 1);
  output.name = value(relation.get(), 0, 2);
  const auto estimate = std::stod(value(relation.get(), 0, 3));
  if (estimate >= 0) {
    output.estimated_rows = estimate;
  }
  output.table_size_bytes = std::stoull(value(relation.get(), 0, 4));
  output.indexes_size_bytes = std::stoull(value(relation.get(), 0, 5));
  output.total_size_bytes = std::stoull(value(relation.get(), 0, 6));

  auto columns = client->queryParams(R"SQL(
    SELECT a.attname, format_type(a.atttypid,a.atttypmod), t.typname, a.atttypmod
    FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
    WHERE a.attrelid=$1::oid AND a.attnum>0 AND NOT a.attisdropped
    ORDER BY a.attnum
  )SQL", {oid});
  for (int row = 0; row < PQntuples(columns.get()); ++row) {
    output.columns.push_back({value(columns.get(), row, 0),
                              value(columns.get(), row, 1),
                              value(columns.get(), row, 2),
                              std::stoi(value(columns.get(), row, 3))});
  }
  auto indexes = client->queryParams(R"SQL(
    SELECT n.nspname, c.relname, am.amname, pg_relation_size(c.oid),
      i.indisvalid, i.indisready,
      array_to_json(ARRAY(SELECT pg_get_indexdef(c.oid,k,true)
        FROM generate_series(1,i.indnkeyatts) k)),
      array_to_json(ARRAY(SELECT opc.opcname FROM unnest(i.indclass)
        WITH ORDINALITY AS oc(oid,pos) JOIN pg_opclass opc ON opc.oid=oc.oid
        ORDER BY oc.pos)),
      COALESCE((SELECT json_object_agg(option_name,option_value)
        FROM pg_options_to_table(c.reloptions)), '{}'::json),
      pg_get_expr(i.indpred,i.indrelid)
    FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_am am ON am.oid=c.relam
    WHERE i.indrelid=$1::oid ORDER BY n.nspname,c.relname
  )SQL", {oid});
  for (int row = 0; row < PQntuples(indexes.get()); ++row) {
    IndexMetadata index;
    index.schema = value(indexes.get(), row, 0);
    index.name = value(indexes.get(), row, 1);
    index.type = value(indexes.get(), row, 2);
    index.size_bytes = std::stoull(value(indexes.get(), row, 3));
    index.valid = value(indexes.get(), row, 4) == "t";
    index.ready = value(indexes.get(), row, 5) == "t";
    index.columns = nlohmann::json::parse(value(indexes.get(), row, 6))
                        .get<std::vector<std::string>>();
    index.operator_classes = nlohmann::json::parse(value(indexes.get(), row, 7))
                                 .get<std::vector<std::string>>();
    index.options = nlohmann::json::parse(value(indexes.get(), row, 8))
                        .get<std::map<std::string, std::string>>();
    if (!PQgetisnull(indexes.get(), row, 9)) {
      index.predicate = value(indexes.get(), row, 9);
    }
    output.indexes.push_back(std::move(index));
  }
  return output;
}

TableResult preflight(const ClientFactory &factory, const DataSet &dataset,
                      const std::string &table, bool query) {
  auto output = inspectTable(factory, table);
  const auto column = std::find_if(output.columns.begin(), output.columns.end(),
      [&](const auto &c) { return c.name == dataset.vector_field_; });
  if (column == output.columns.end()) {
    throw std::runtime_error("vector column " + dataset.vector_field_ +
                             " not found in table " + table);
  }
  if (column->type_name != typeName(dataset.storage_type_) || column->dimensions != dataset.dim_) {
    throw std::runtime_error("table " + table + " column " + dataset.vector_field_ +
        " has type " + column->type + "; expected " + sqlType(dataset.storage_type_, dataset.dim_) + " for dataset " + dataset.name_);
  }
  if (query) {
    const auto id = std::find_if(output.columns.begin(), output.columns.end(),
        [](const auto &c) { return c.name == "id"; });
    if (id == output.columns.end() ||
        (id->type_name != "int2" && id->type_name != "int4" && id->type_name != "int8")) {
      throw std::runtime_error("table " + table + " requires an integer id column");
    }
    const auto expected = indexOpclass(dataset);
    auto normalized = [](std::string text) {
      std::erase_if(text, [](unsigned char c) { return std::isspace(c) || c == '(' || c == ')' || c == '"'; });
      return text;
    };
    const auto expression = normalized(indexExpression(dataset, quoteIdentifier(dataset.vector_field_)));
    bool matching_index = false;
    for (const auto &index : output.indexes) {
      if (!index.valid || !index.ready || index.predicate ||
          (index.type != "hnsw" && index.type != "ivfflat")) {
        continue;
      }
      for (size_t i = 0; i < index.columns.size(); ++i) {
        if (normalized(index.columns[i]) == expression &&
            i < index.operator_classes.size() && index.operator_classes[i] == expected) {
          matching_index = true;
        }
      }
    }
    if (!matching_index) {
      output.warnings.push_back("No valid non-partial ANN index matches " +
          dataset.vector_field_ + " " + std::string(expected) +
          "; the query may use a sequential scan");
    }
  }
  for (const auto &warning : output.warnings) {
    SPDLOG_WARN("{}", warning);
  }
  return output;
}

} // namespace pgvectorbench
