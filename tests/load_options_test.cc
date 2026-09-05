#include <cstdlib>
#include <limits>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>
#include <spdlog/sinks/stdout_sinks.h>

#include "dataset/dataset.h"
#include "phases.h"
#include "utils/client_factory.h"

namespace {

class LoadOptionsTest : public testing::TestWithParam<const char *> {};

TEST_P(LoadOptionsTest, RejectsInvalidValuesBeforeStartingWorkers) {
  const pgvectorbench::DataSet dataset(
      "/nonexistent/", "load_options", pgvectorbench::DataSetFormat::FVECS_FORMAT,
      pgvectorbench::DataSetBaseType::FLOAT, pgvectorbench::DataSetMetric::L2,
      {{"base.fvecs", 1}}, {}, {}, "embedding", 128, 1, {}, {}, 0);
  auto factory = pgvectorbench::ClientFactory::createBuilder().build();

  for (const std::string value : {"", "0", "-1", "+1", "1x", "1.5", " 1",
                                  "18446744073709551615",
                                  "18446744073709551616"}) {
    SCOPED_TRACE(std::string(GetParam()) + "=" + value);
    std::unordered_map<std::string, std::string> options{
        {"batch_size", "1"}, {"thread_num", "1"}, {"client_num", "1"},
        {"queue_capacity", "1"}};
    options[GetParam()] = value;
    ASSERT_EXIT(
        {
          spdlog::set_default_logger(spdlog::stderr_logger_mt("load_options"));
          pgvectorbench::load(&dataset, factory.get(), options);
          std::exit(0);
        },
        testing::ExitedWithCode(1),
        std::string("Invalid load option ") + GetParam());
  }
}

INSTANTIATE_TEST_SUITE_P(PositiveIntegers, LoadOptionsTest,
                        testing::Values("batch_size", "thread_num", "client_num",
                                        "queue_capacity"));

TEST(LoadOptionsBoundaryTest, RejectsOverflowingVecsBatchBuffer) {
  const pgvectorbench::DataSet dataset(
      "/nonexistent/", "load_options", pgvectorbench::DataSetFormat::FVECS_FORMAT,
      pgvectorbench::DataSetBaseType::FLOAT, pgvectorbench::DataSetMetric::L2,
      {{"base.fvecs", 1}}, {}, {}, "embedding", 128, 1, {}, {}, 0);
  auto factory = pgvectorbench::ClientFactory::createBuilder().build();
  const size_t row_size = sizeof(uint32_t) + 128 * sizeof(float);
  const auto batch_size =
      std::to_string(std::numeric_limits<size_t>::max() / row_size + 1);
  ASSERT_EXIT(
      {
        spdlog::set_default_logger(spdlog::stderr_logger_mt("load_options"));
        pgvectorbench::load(&dataset, factory.get(), {{"batch_size", batch_size}});
        std::exit(0);
      },
      testing::ExitedWithCode(1), "Invalid load option batch_size");
}

} // namespace
