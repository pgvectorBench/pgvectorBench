#include <memory>
#include <sstream>
#include <unordered_set>

#include "dataset/dataset.h"
#include "utils/client_factory.h"
#include "utils/parser.h"
#include "utils/util.h"

namespace pgvectorbench {

extern void
create_index(const DataSet *dataset, const ClientFactory *cf,
             const std::unordered_map<std::string, std::string> &index_opt_map);

namespace {

std::string generateCreateExtensionStatement(const std::string &extension) {
  std::ostringstream oss;
  oss << "CREATE EXTENSION IF NOT EXISTS " << extension << ";";
  std::string statement = oss.str();

  SPDLOG_DEBUG("create extension statement: {}", statement);
  return statement;
}

std::string
generateCreateTableStatement(const DataSet *dataset,
                             const std::optional<std::string> &table_name) {
  std::ostringstream oss;
  oss << "CREATE TABLE "
      << (table_name.has_value() ? table_name.value() : dataset->name_) << "(";
  bool flag = true;
  for (const auto &field : dataset->fields_) {
    if (flag) {
      flag = false;
    } else {
      oss << ',';
    }
    oss << "\n    " << field.first << " " << field.second;
  }

  oss << "\n);";
  std::string statement = oss.str();

  SPDLOG_DEBUG("create table statement: {}", statement);
  return statement;
}

} // namespace

void setup(const DataSet *dataset, const ClientFactory *cf,
           const std::unordered_map<std::string, std::string> &setup_opt_map) {
  assert(dataset != nullptr);
  assert(cf != nullptr);

  auto client = cf->createClient();
  assert(client != nullptr);

  auto table_name = Util::getValueFromMap(setup_opt_map, "table_name");

  // create extensions
  std::vector<std::string> extensions;
  auto e = Util::getValueFromMap(setup_opt_map, "extension");
  if (e.has_value()) {
    extensions.push_back(e.value());
  }
  e = Util::getValueFromMap(setup_opt_map, "extensions");
  if (e.has_value()) {
    CSVParser::parseLine(
        e.value(), [&](std::string &token) { extensions.push_back(token); });
  }

  for (auto &extension : extensions) {
    auto statement = generateCreateExtensionStatement(extension);
    auto ret =
        client->executeQuery(statement.c_str(), [&](PGresult *res) -> bool {
          // no need to handle result
          assert(PQresultStatus(res) == PGRES_COMMAND_OK);
          SPDLOG_INFO("create extension succeeded: {}", statement);
          return true;
        });

    if (!ret) {
      SPDLOG_ERROR("failed at setup phase when creating extension {}",
                   extension);
      std::exit(1);
    }
  }

  auto statement = generateCreateTableStatement(dataset, table_name);
  auto ret =
      client->executeQuery(statement.c_str(), [&](PGresult *res) -> bool {
        // no need to handle result
        assert(PQresultStatus(res) == PGRES_COMMAND_OK);
        SPDLOG_INFO("create table succeeded: {}", statement);
        return true;
      });

  if (!ret) {
    SPDLOG_ERROR("failed at setup phase when creating table");
    std::exit(1);
  }

  // create index in setup phase, this is ahead of loading phase
  auto index_name = Util::getValueFromMap(setup_opt_map, "index_name");
  if (index_name.has_value()) {
    SPDLOG_INFO("start creating index in setup phase");
    create_index(dataset, cf, setup_opt_map);
    SPDLOG_INFO("end of creating index in setup phase");
  }
}

} // namespace pgvectorbench
