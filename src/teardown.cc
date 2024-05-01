#include <memory>
#include <sstream>

#include "dataset/dataset.h"
#include "utils/client_factory.h"
#include "utils/parser.h"
#include "utils/util.h"

namespace pgvectorbench {

extern void drop_index(const DataSet *dataset, const ClientFactory *cf,
                       const std::optional<std::string> &index_name);

namespace {

std::string generateDropExtensionStatement(const std::string &extension) {
  std::ostringstream oss;
  oss << "DROP EXTENSION IF EXISTS " << extension << ";";
  std::string statement = oss.str();

  SPDLOG_DEBUG("drop extension statement: {}", statement);
  return statement;
}

std::string
generateTruncateTableStatement(const DataSet *dataset,
                               const std::optional<std::string> &table_name) {
  std::ostringstream oss;
  oss << "TRUNCATE TABLE "
      << (table_name.has_value() ? table_name.value() : dataset->name_) << ";";
  std::string statement = oss.str();

  SPDLOG_DEBUG("truncate table statement: {}", statement);
  return statement;
}

std::string
generateDropTableStatement(const DataSet *dataset,
                           const std::optional<std::string> &table_name) {
  std::ostringstream oss;
  oss << "DROP TABLE IF EXISTS "
      << (table_name.has_value() ? table_name.value() : dataset->name_) << ";";
  std::string statement = oss.str();

  SPDLOG_DEBUG("drop table statement: {}", statement);
  return statement;
}

} // namespace

void teardown(
    const DataSet *dataset, const ClientFactory *cf,
    const std::unordered_map<std::string, std::string> &teardown_opt_map) {
  assert(dataset != nullptr);
  assert(cf != nullptr);

  auto client = cf->createClient();
  assert(client != nullptr);

  auto table_name = Util::getValueFromMap(teardown_opt_map, "table_name");
  auto truncate = Util::getValueFromMap(teardown_opt_map, "truncate");
  bool do_truncate = false;
  if (truncate.has_value()) {
    std::string lowercase = truncate.value();
    std::transform(lowercase.begin(), lowercase.end(), lowercase.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (lowercase == "yes" || lowercase == "y") {
      do_truncate = true;
    }
  }

  auto drop_index_opt = Util::getValueFromMap(teardown_opt_map, "drop_index");
  bool do_drop_index = false;
  if (drop_index_opt.has_value()) {
    std::string lowercase = drop_index_opt.value();
    std::transform(lowercase.begin(), lowercase.end(), lowercase.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (lowercase == "yes" || lowercase == "y") {
      do_drop_index = true;
    }
  }

  // handle truncate
  if (do_truncate) {
    auto statement = generateTruncateTableStatement(dataset, table_name);
    auto ret =
        client->executeQuery(statement.c_str(), [&](PGresult *res) -> bool {
          // no need to handle result
          assert(PQresultStatus(res) == PGRES_COMMAND_OK);
          SPDLOG_INFO("truncate table succeeded: {}", statement);
          return true;
        });
    if (!ret) {
      SPDLOG_ERROR("failed at setup phase when truncating table");
      std::exit(1);
    }
  }

  // handle drop index
  if (do_drop_index) {
    auto index_name = Util::getValueFromMap(teardown_opt_map, "index_name");
    drop_index(dataset, cf, index_name);
  }

  // no need to drop extensions and table for truncate or drop index
  if (do_truncate || do_drop_index) {
    return;
  }

  // drop table
  auto statement = generateDropTableStatement(dataset, table_name);
  auto ret =
      client->executeQuery(statement.c_str(), [&](PGresult *res) -> bool {
        // no need to handle result
        assert(PQresultStatus(res) == PGRES_COMMAND_OK);
        SPDLOG_INFO("drop table succeeded: {}", statement);
        return true;
      });

  if (!ret) {
    SPDLOG_ERROR("failed at teardown phase when droping table");
    std::exit(1);
  }

  // drop extensions
  std::vector<std::string> extensions;
  auto e = Util::getValueFromMap(teardown_opt_map, "extension");
  if (e.has_value()) {
    extensions.push_back(e.value());
  }
  e = Util::getValueFromMap(teardown_opt_map, "extensions");
  if (e.has_value()) {
    CSVParser::parseLine(
        e.value(), [&](std::string &token) { extensions.push_back(token); });
  }

  for (auto &extension : extensions) {
    auto statement = generateDropExtensionStatement(extension);
    auto ret =
        client->executeQuery(statement.c_str(), [&](PGresult *res) -> bool {
          // no need to handle result
          assert(PQresultStatus(res) == PGRES_COMMAND_OK);
          SPDLOG_INFO("drop extension succeeded: {}", statement);
          return true;
        });

    if (!ret) {
      SPDLOG_ERROR("failed at teardown phase when droping extension {}",
                   extension);
      std::exit(1);
    }
  }
}

} // namespace pgvectorbench
