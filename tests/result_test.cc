#include <cstdint>
#include <limits>
#include <locale>
#include <memory>
#include <sstream>
#include <stdexcept>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include "result.h"

namespace pgvectorbench {
namespace {

TEST(ResultTest, TextLogsPreserveIntegerLatenciesAndFractionalMetrics) {
  std::ostringstream output;
  auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(output);
  auto logger = std::make_shared<spdlog::logger>("result_test", sink);
  logger->set_pattern("%v");
  struct RestoreLogger {
    std::shared_ptr<spdlog::logger> previous;
    ~RestoreLogger() { spdlog::set_default_logger(previous); }
  } restore{spdlog::default_logger()};
  spdlog::set_default_logger(logger);

  QueryResult result;
  result.latency_us = {2, 1234567, 2345678, 1790122.5,
                       {{"99.0", 99, 2345678}}};
  result.recall = {2, 1, 0.25, 0.625, {{"99.0", 99, 0.25}}};
  logQueryResult(result);
  EXPECT_NE(output.str().find("latency(us): best=1234567 worst=2345678 "
                             "average=1.79012e+06 P(99.0%)=2345678\n"),
            std::string::npos)
      << output.str();
  EXPECT_NE(output.str().find("recall: best=1 worst=0.25 average=0.625 "
                             "P(99.0%)=0.25\n"),
            std::string::npos)
      << output.str();

  result.latency_us = {2, 1, 2, 1.5, {{"50", 50, 1}}};
  logQueryResult(result);
  EXPECT_NE(output.str().find("latency(us): best=1 worst=2 average=1.5 "
                             "P(50%)=1\n"),
            std::string::npos)
      << output.str();
}

TEST(ResultTest, EscapesStringsAndPreservesUnicode) {
  BenchmarkResult result;
  result.tool_version = "test";
  result.query.dataset.name = "数据\"\\\n\t\r\b\f";
  result.query.config.table_name = std::string("a\0b", 3);
  result.query.config.session_overrides["a\"b"] = "c\\d";
  const auto json = nlohmann::json::parse(resultToJson(result));
  EXPECT_EQ(json.at("query").at("dataset").at("name"), result.query.dataset.name);
  const auto &config = json.at("query").at("config");
  EXPECT_EQ(config.at("table_name"), result.query.config.table_name);
  EXPECT_EQ(config.at("session_overrides"), result.query.config.session_overrides);
  EXPECT_EQ(json.at("status"), "success");
}

class CommaDecimal : public std::numpunct<char> {
protected:
  char do_decimal_point() const override { return ','; }
};

TEST(ResultTest, NumbersAreLocaleIndependentAndKeepPrecision) {
  BenchmarkResult result;
  result.query.qps = 1.2345678901234567;
  result.query.latency_us.count = 9007199254740993ULL;
  const auto previous = std::locale();
  struct RestoreLocale {
    std::locale value;
    ~RestoreLocale() { std::locale::global(value); }
  } restore{previous};
  std::locale::global(std::locale(previous, new CommaDecimal));
  const auto json = nlohmann::json::parse(resultToJson(result));
  EXPECT_DOUBLE_EQ(json.at("query").at("qps").get<double>(), result.query.qps);
  EXPECT_EQ(json.at("query").at("latency_us").at("count").get<uint64_t>(),
            result.query.latency_us.count);
}

TEST(ResultTest, RejectsNonFiniteMetrics) {
  for (auto value : {std::numeric_limits<double>::infinity(),
                     -std::numeric_limits<double>::infinity(),
                     std::numeric_limits<double>::quiet_NaN()}) {
    BenchmarkResult result;
    result.query.qps = value;
    EXPECT_THROW(resultToJson(result), std::runtime_error);
    result.query.qps = 1;
    result.query.recall.percentiles = {{"90", 90, value}};
    EXPECT_THROW(resultToJson(result), std::runtime_error);
    result.query.recall.percentiles = {{"90", value, 1}};
    EXPECT_THROW(resultToJson(result), std::runtime_error);
  }
}

TEST(ResultTest, KeepsRepeatedPercentilesAsNumericArrayEntries) {
  BenchmarkResult result;
  result.query.recall.percentiles = {{"90", 90, 0.5}, {"90.0", 90, 0.5}};
  const auto json = nlohmann::json::parse(resultToJson(result));
  const auto expected = nlohmann::json::array(
      {{{"percentage", 90}, {"value", 0.5}},
       {{"percentage", 90}, {"value", 0.5}}});
  EXPECT_EQ(json.at("query").at("recall").at("percentiles"), expected);
  EXPECT_EQ(json.at("query").at("latency_us").at("percentiles"),
            nlohmann::json::array());
}

} // namespace
} // namespace pgvectorbench
