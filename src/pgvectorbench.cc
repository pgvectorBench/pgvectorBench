#include <argparse/argparse.hpp>
#include <filesystem>
#include <memory>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include "dataset/dataset.h"
#include "utils/client_factory.h"
#include "utils/parser.h"
#include "utils/util.h"

#define PGVECTORBENCH_VERSION "0.1.0"

namespace fs = std::filesystem;
using pgvectorbench::Util;

namespace pgvectorbench {

extern void
setup(const DataSet *dataset, const ClientFactory *cf,
      const std::unordered_map<std::string, std::string> &setup_opt_map);
extern void
load(const DataSet *dataset, const ClientFactory *cf,
     const std::unordered_map<std::string, std::string> &load_opt_map);
extern void
create_index(const DataSet *dataset, const ClientFactory *cf,
             const std::unordered_map<std::string, std::string> &index_opt_map);
extern void
query(const DataSet *dataset, const ClientFactory *cf,
      const std::unordered_map<std::string, std::string> &query_opt_map);
extern void
teardown(const DataSet *dataset, const ClientFactory *cf,
         const std::unordered_map<std::string, std::string> &teardown_opt_map);
} // namespace pgvectorbench

int main(int argc, char **argv) {

  // change the current working directory
  auto path = fs::current_path(); // getting path
  fs::current_path(path);         // setting path

  argparse::ArgumentParser program("pgvectorbench", PGVECTORBENCH_VERSION);

  // connection options
  program.add_argument("-h", "--host")
      .help("database server host or socket directory");
  program.add_argument("-p", "--port").help("database server port");
  program.add_argument("-U", "--username").help("database user name");
  program.add_argument("-W", "--password")
      .help("password for the specified user");
  program.add_argument("-d", "--dbname").help("database name to connect to");

  // dataset
  program.add_argument("-D", "--dataset")
      .default_value(std::string("siftsmall"))
      .choices("siftsmall", "sift", "gist", "glove", "crawl", "deep1B",
               "cohere_small_100k", "cohere_small_100k_filter1",
               "cohere_small_100k_filter99", "cohere_medium_1m",
               "cohere_medium_1m_filter1", "cohere_medium_1m_filter99",
               "cohere_large_10m", "cohere_large_10m_filter1",
               "cohere_large_10m_filter99", "openai_small_50k",
               "openai_small_50k_filter1", "openai_small_50k_filter99",
               "openai_medium_500k", "openai_medium_500k_filter1",
               "openai_medium_500k_filter99", "openai_large_5m",
               "openai_large_5m_filter1", "openai_large_5m_filter99",
               "laion_large_100m")
      .help("dataset name used to run the benchmark");

  // dataset path
  program.add_argument("-P", "--path").help("dataset path");

  // log file name & log level
  program.add_argument("-l", "--log").help("send log to file");

  // setup benmarking table and may be some gucs
  program.add_argument("--setup").default_value("").help(
      "k/v pairs seperated by semicolon for setup options");

  // load
  program.add_argument("--load").default_value("").help(
      "k/v pairs seperated by semicolon for loading dataset");

  // index
  program.add_argument("--index").help(
      "k/v pairs seperated by semicolon for creating index");

  // query
  program.add_argument("--query").default_value("").help(
      "k/v pairs seperated by semicolon for running the benchmarking queries");

  // teardown
  program.add_argument("--teardown")
      .default_value("")
      .help("k/v pairs seperated by semicolon for teardown options");

  try {
    program.parse_args(argc, argv);
  } catch (const std::exception &err) {
    spdlog::error("Error: \n  {}\n\n {}", err.what(), program.help().str());
    std::exit(1);
  }

  spdlog::set_pattern("[%Y-%m-%d %T.%f] [%l] [%s:%# thread %t] %v");
  // change log level and default logger
  if (SPDLOG_ACTIVE_LEVEL == SPDLOG_LEVEL_DEBUG) {
    spdlog::set_level(spdlog::level::debug);
  }

  if (auto log = program.present("-l")) {
    auto new_logger = spdlog::basic_logger_mt("file_logger", *log);
    spdlog::set_default_logger(new_logger);
  }

  // build ClientFactory
  auto cf_builder = pgvectorbench::ClientFactory::createBuilder();

  if (auto host = program.present("--host")) {
    cf_builder.setHost(*host);
  }

  if (auto port = program.present("--port")) {
    cf_builder.setPort(*port);
  }

  if (auto user = program.present("--username")) {
    cf_builder.setUser(*user);
  }

  if (auto password = program.present("--password")) {
    cf_builder.setPassword(*password);
  }

  if (auto dbname = program.present("--dbname")) {
    cf_builder.setDBName(*dbname);
  }

  auto cf = cf_builder.build();
  if (!cf->pingServer()) {
    std::exit(1);
  }

  // get DataSet
  std::string dataset = program.get<std::string>("--dataset");
  auto ds = pgvectorbench::getDataSet(dataset);
  if (ds == nullptr) {
    SPDLOG_ERROR("dataset {} not exists", dataset);
    std::exit(1);
  }
  if (auto location = program.present("--path")) {
    if (fs::exists(*location) && fs::is_directory(*location)) {
      auto location_with_slash = *location;
      if (location_with_slash.back() != '/') {
        location_with_slash.push_back('/');
      }
      ds->set_location(location_with_slash);
    } else {
      SPDLOG_ERROR("Illegal dataset path");
      std::exit(1);
    }
  }
  SPDLOG_INFO("dataset: \n{}", *ds);

  bool index_created = false;
  if (program.is_used("--setup")) {
    auto setup_opt = program.get<std::string>("--setup");
    std::unordered_map<std::string, std::string> setup_opt_map;
    pgvectorbench::CSVParser::parseLine(
        setup_opt,
        [&](std::string &token) {
          auto pos = token.find('=');
          if (pos != std::string::npos) {
            std::string key = token.substr(0, pos);
            std::string value = token.substr(pos + 1);
            setup_opt_map.emplace(std::move(key), std::move(value));
          }
        },
        ';');
    if (Util::getValueFromMap(setup_opt_map, "index_type").has_value()) {
      // index created in setup phase
      index_created = true;
    }
    SPDLOG_INFO("start setting up the benchmarking table");
    pgvectorbench::setup(ds, cf.get(), setup_opt_map);
    SPDLOG_INFO("end of setting up");
  }

  if (program.is_used("--load")) {
    auto load_opt = program.get<std::string>("--load");
    std::unordered_map<std::string, std::string> load_opt_map;
    pgvectorbench::CSVParser::parseLine(
        load_opt,
        [&](std::string &token) {
          auto pos = token.find('=');
          if (pos != std::string::npos) {
            std::string key = token.substr(0, pos);
            std::string value = token.substr(pos + 1);
            load_opt_map.emplace(std::move(key), std::move(value));
          }
        },
        ';');
    SPDLOG_INFO("start loading");
    pgvectorbench::load(ds, cf.get(), load_opt_map);
    SPDLOG_INFO("end of loading");
  }

  // index has not been created in setup phase
  if (!index_created && program.is_used("--index")) {
    auto index_opt = program.get<std::string>("--index");
    std::unordered_map<std::string, std::string> index_opt_map;
    pgvectorbench::CSVParser::parseLine(
        index_opt,
        [&](std::string &token) {
          auto pos = token.find('=');
          if (pos != std::string::npos) {
            std::string key = token.substr(0, pos);
            std::string value = token.substr(pos + 1);
            index_opt_map.emplace(std::move(key), std::move(value));
          }
        },
        ';');

    SPDLOG_INFO("start creating index");
    pgvectorbench::create_index(ds, cf.get(), index_opt_map);
    SPDLOG_INFO("end of creating index");
  }

  if (program.is_used("--query")) {
    auto query_opt = program.get<std::string>("--query");
    std::unordered_map<std::string, std::string> query_opt_map;
    pgvectorbench::CSVParser::parseLine(
        query_opt,
        [&](std::string &token) {
          auto pos = token.find('=');
          if (pos != std::string::npos) {
            std::string key = token.substr(0, pos);
            std::string value = token.substr(pos + 1);
            query_opt_map.emplace(std::move(key), std::move(value));
          }
        },
        ';');
    SPDLOG_INFO("start querying");
    pgvectorbench::query(ds, cf.get(), query_opt_map);
    SPDLOG_INFO("end of queryring");
  }

  if (program.is_used("--teardown")) {
    auto teardown_opt = program.get<std::string>("--teardown");
    std::unordered_map<std::string, std::string> teardown_opt_map;
    pgvectorbench::CSVParser::parseLine(
        teardown_opt,
        [&](std::string &token) {
          auto pos = token.find('=');
          if (pos != std::string::npos) {
            std::string key = token.substr(0, pos);
            std::string value = token.substr(pos + 1);
            teardown_opt_map.emplace(std::move(key), std::move(value));
          }
        },
        ';');
    SPDLOG_INFO("start tearing down the benchmaking table");
    pgvectorbench::teardown(ds, cf.get(), teardown_opt_map);
    SPDLOG_INFO("end of tearing down");
  }

  return 0;
}
