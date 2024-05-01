#include <optional>
#include <sstream>

#include "dataset/dataset.h"
#include "utils/client_factory.h"
#include "utils/util.h"

namespace pgvectorbench {

namespace {

std::string generateCreateHNSWIndexStatement(
    const DataSet *dataset, const std::optional<std::string> &index_name,
    const std::optional<std::string> &table_name,
    const std::optional<std::string> &m,
    const std::optional<std::string> &ef_construction) {
  std::ostringstream oss;
  oss << "CREATE INDEX ";
  if (index_name.has_value()) {
    oss << index_name.value();
  } else {
    oss << dataset->name_ << "_" << dataset->vector_field_ << "_idx";
  }
  oss << " ON "
      << (table_name.has_value() ? table_name.value() : dataset->name_)
      << " USING hnsw (" << dataset->vector_field_ << " "
      << metric2ops(dataset->metric_) << ")";

  if (m.has_value() || ef_construction.has_value()) {
    oss << " WITH (";
    if (m.has_value()) {
      oss << "m = " << m.value();
      if (ef_construction.has_value()) {
        oss << ", ef_construction = " << ef_construction.value();
      }
    } else if (ef_construction.has_value()) {
      oss << "ef_construction = " << ef_construction.value();
    }
    oss << ")";
  }

  oss << ";";

  std::string statement = oss.str();

  SPDLOG_DEBUG("create index statement: {}", statement);
  return statement;
}

std::string
generateCreateIVFFlatIndexStatement(const DataSet *dataset,
                                    std::optional<std::string> &index_name,
                                    std::optional<std::string> &table_name,
                                    const std::optional<std::string> &lists) {
  std::ostringstream oss;
  oss << "CREATE INDEX ";
  if (index_name.has_value()) {
    oss << index_name.value();
  } else {
    oss << dataset->name_ << "_" << dataset->vector_field_ << "_idx";
  }
  oss << " ON "
      << (table_name.has_value() ? table_name.value() : dataset->name_)
      << " USING ivfflat (" << dataset->vector_field_ << " "
      << metric2ops(dataset->metric_) << ")";

  if (lists.has_value()) {
    oss << " WITH (lists = " << lists.value() << ")";
  }

  oss << ";";

  std::string statement = oss.str();

  SPDLOG_DEBUG("create index statement: {}", statement);
  return statement;
}

std::string
generateDropIndexStatement(const DataSet *dataset,
                           const std::optional<std::string> &index_name) {
  std::ostringstream oss;
  oss << "DROP INDEX IF EXISTS ";

  if (index_name.has_value()) {
    oss << index_name.value();
  } else {
    oss << dataset->name_ << "_" << dataset->vector_field_ << "_idx";
  }
  oss << ";";
  std::string statement = oss.str();

  SPDLOG_DEBUG("drop index statement: {}", statement);
  return statement;
}

std::vector<std::string> generateIndexOptions(
    const std::unordered_map<std::string, std::string> &index_opt_map) {
  std::vector<std::string> sqls;

  // Indexes build significantly faster when the graph fits into
  // maintenance_work_mem
  if (index_opt_map.find("maintenance_work_mem") != index_opt_map.end()) {
    sqls.push_back(fmt::format("SET maintenance_work_mem = '{}';",
                               index_opt_map.at("maintenance_work_mem")));
  }

  // parallel
  if (index_opt_map.find("max_parallel_maintenance_workers") !=
      index_opt_map.end()) {
    sqls.push_back(
        fmt::format("SET max_parallel_maintenance_workers = {};",
                    index_opt_map.at("max_parallel_maintenance_workers")));
  }

  return sqls;
}

} // namespace

void create_index(
    const DataSet *dataset, const ClientFactory *cf,
    const std::unordered_map<std::string, std::string> &index_opt_map) {
  assert(dataset != nullptr);
  assert(cf != nullptr);

  auto client = cf->createClient();
  assert(client != nullptr);

  // set index build options
  std::vector<std::string> indexOptions = generateIndexOptions(index_opt_map);
  for (const auto &indexOption : indexOptions) {
    auto ret =
        client->executeQuery(indexOption.c_str(), [&](PGresult *res) -> bool {
          // no need to handle result
          assert(PQresultStatus(res) == PGRES_COMMAND_OK);
          SPDLOG_INFO("successfully excuted: {}", indexOption);
          return true;
        });
    if (!ret) {
      SPDLOG_ERROR("failed to execute: {}", indexOption);
    }
  }

  auto table_name = Util::getValueFromMap(index_opt_map, "table_name");
  auto index_name = Util::getValueFromMap(index_opt_map, "index_name");

  std::string statement;

  auto index_type = Util::getValueFromMap(index_opt_map, "index_type");
  assert(index_type.has_value());
  std::string index_type_lower_case = index_type.value();
  std::transform(index_type_lower_case.begin(), index_type_lower_case.end(),
                 index_type_lower_case.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  if (index_type_lower_case == "hnsw") {
    auto m = Util::getValueFromMap(index_opt_map, "m");
    auto ef_construction =
        Util::getValueFromMap(index_opt_map, "ef_construction");
    statement = generateCreateHNSWIndexStatement(
        dataset, index_name, table_name, m, ef_construction);
  } else if (index_type_lower_case == "ivfflat") {
    auto lists = Util::getValueFromMap(index_opt_map, "lists");
    statement = generateCreateIVFFlatIndexStatement(dataset, index_name,
                                                    table_name, lists);
  } else {
    SPDLOG_ERROR("index type: {} not supported in pgvector");
    std::exit(1);
  }

  auto ret =
      client->executeQuery(statement.c_str(), [&](PGresult *res) -> bool {
        // no need to handle result
        assert(PQresultStatus(res) == PGRES_COMMAND_OK);
        SPDLOG_INFO("create index succeeded: {}", statement);
        return true;
      });
  if (!ret) {
    SPDLOG_ERROR("failed when creating index");
    std::exit(1);
  }
}

void drop_index(const DataSet *dataset, const ClientFactory *cf,
                const std::optional<std::string> &index_name) {
  assert(dataset != nullptr);
  assert(cf != nullptr);

  auto client = cf->createClient();
  assert(client != nullptr);

  auto statement = generateDropIndexStatement(dataset, index_name);
  auto ret =
      client->executeQuery(statement.c_str(), [&](PGresult *res) -> bool {
        // no need to handle result
        assert(PQresultStatus(res) == PGRES_COMMAND_OK);
        SPDLOG_INFO("drop index succeeded: {}", statement);
        return true;
      });
  if (!ret) {
    SPDLOG_ERROR("failed when dropping index");
    std::exit(1);
  }
}

} // namespace pgvectorbench
