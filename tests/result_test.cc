#include <cstdint>
#include <limits>
#include <locale>
#include <stdexcept>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "result.h"

namespace pgvectorbench {
namespace {

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
