#include <argparse/argparse.hpp>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include "dataset/dataset.h"
#include "query.h"
#include "environment.h"
#include "phases.h"
#include "utils/client_factory.h"
#include "utils/parser.h"
#include "utils/util.h"

#define PGVECTORBENCH_VERSION "0.1.0"

namespace fs = std::filesystem;
using pgvectorbench::Util;

namespace {
pgvectorbench::PhaseOptions phaseOptions(const argparse::ArgumentParser &program,
                                        const std::string &phase) {
  pgvectorbench::PhaseOptions options;
  pgvectorbench::CSVParser::parseLine(program.get<std::string>(phase),
      [&](std::string &token) {
        auto pos = token.find('=');
        if (pos != std::string::npos) {
          options.emplace(token.substr(0, pos), token.substr(pos + 1));
        }
      }, ';');
  return options;
}
} // namespace

int main(int argc, char **argv) try {

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
  program.add_argument("--json")
      .default_value(false)
      .implicit_value(true)
      .help("write results as JSON to stdout (requires at least one phase)");

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
    std::cerr << "Error: \n  " << err.what() << "\n\n " << program.help().str();
    return 1;
  }

  const bool json_output = program.get<bool>("--json");
  if (json_output) {
    spdlog::set_default_logger(spdlog::stderr_logger_mt("json_diagnostics"));
    if (!program.is_used("--setup") && !program.is_used("--load") &&
        !program.is_used("--index") && !program.is_used("--query") &&
        !program.is_used("--teardown")) {
      SPDLOG_ERROR("--json requires at least one benchmark phase");
      return 1;
    }
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
  SPDLOG_INFO("dataset: \n{}", fmt::streamed(*ds));

  pgvectorbench::BenchmarkResult result;
  result.tool_version = PGVECTORBENCH_VERSION;
  result.environment = pgvectorbench::inspectEnvironment(*cf);
  SPDLOG_INFO("database: {}, PostgreSQL: {}, pgvector: {}",
              result.environment->database, result.environment->postgres_version,
              result.environment->pgvector_version.value_or("not installed"));

  if (program.is_used("--setup")) {
    auto options = phaseOptions(program, "--setup");
    SPDLOG_INFO("start setting up the benchmarking table");
    result.setup = pgvectorbench::setup(ds, cf.get(), options);
    result.setup->table = pgvectorbench::preflight(*cf, *ds, result.setup->table_name);
    if (result.setup->index) result.setup->index->table = result.setup->table;
    result.environment = pgvectorbench::inspectEnvironment(*cf);
    SPDLOG_INFO("end of setting up");
  }

  if (program.is_used("--load")) {
    auto options = phaseOptions(program, "--load");
    const auto table = Util::getValueFromMap(options, "table_name").value_or(ds->name_);
    pgvectorbench::preflight(*cf, *ds, table);
    SPDLOG_INFO("start loading");
    result.load = pgvectorbench::load(ds, cf.get(), options);
    result.load->table = pgvectorbench::inspectTable(*cf, table);
    SPDLOG_INFO("end of loading");
  }

  if (program.is_used("--index") && !(result.setup && result.setup->index)) {
    auto options = phaseOptions(program, "--index");
    const auto table = Util::getValueFromMap(options, "table_name").value_or(ds->name_);
    pgvectorbench::preflight(*cf, *ds, table);
    SPDLOG_INFO("start creating index");
    result.index = pgvectorbench::create_index(ds, cf.get(), options);
    result.index->table = pgvectorbench::inspectTable(*cf, table);
    SPDLOG_INFO("end of creating index");
  }

  if (program.is_used("--query")) {
    auto options = phaseOptions(program, "--query");
    const auto table = Util::getValueFromMap(options, "table_name").value_or(ds->name_);
    auto metadata = pgvectorbench::preflight(*cf, *ds, table, true);
    SPDLOG_INFO("start querying");
    result.query = pgvectorbench::query(ds, cf.get(), options);
    result.query->table = std::move(metadata);
    pgvectorbench::logQueryResult(*result.query);
    SPDLOG_INFO("end of querying");
  }

  if (program.is_used("--teardown")) {
    auto options = phaseOptions(program, "--teardown");
    pgvectorbench::PhaseResult phase;
    phase.table_name = Util::getValueFromMap(options, "table_name").value_or(ds->name_);
    // Teardown accepts an already absent table (DROP IF EXISTS). Earlier phases
    // retain their own snapshots, even when teardown removes the table/index.
    const auto start = std::chrono::steady_clock::now();
    SPDLOG_INFO("start tearing down the benchmarking table");
    pgvectorbench::teardown(ds, cf.get(), options);
    phase.elapsed_seconds = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - start).count();
    result.teardown = std::move(phase);
    SPDLOG_INFO("end of tearing down");
  }

  // Delay JSON until all requested phases have succeeded, including teardown.
  if (json_output) {
    std::cout << pgvectorbench::resultToJson(result);
    std::cout.flush();
    if (!std::cout) {
      throw std::runtime_error("failed to write JSON results");
    }
  }
  return 0;
} catch (const std::exception &error) {
  SPDLOG_ERROR("benchmark failed: {}", error.what());
  return 1;
}
