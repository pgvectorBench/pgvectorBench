#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>

#include "dataset/datasource.h"

namespace pgvectorbench {
namespace {

class DataSourceTest : public testing::Test {
protected:
  void SetUp() override {
    auto pattern = (std::filesystem::temp_directory_path() /
                    "pgvectorbench-datasource-XXXXXX").string();
    ASSERT_NE(mkdtemp(pattern.data()), nullptr);
    directory_ = pattern;
  }

  void TearDown() override {
    if (!directory_.empty()) {
      std::filesystem::remove_all(directory_);
    }
  }

  DataSet dataset(DataSetFormat format, size_t rows = 2) {
    return DataSet(directory_ + "/", "test", format, DataSetBaseType::FLOAT,
                   DataSetMetric::L2, {{"base", rows}}, {}, {}, "embedding",
                   1, rows, {"query", 0}, {"gt", 0}, 0);
  }

  void writeVecs() {
    std::ofstream file(directory_ + "/base", std::ios::binary);
    const uint32_t dimension = 1;
    for (float value : {1.0f, 2.0f}) {
      file.write(reinterpret_cast<const char *>(&dimension), sizeof(dimension));
      file.write(reinterpret_cast<const char *>(&value), sizeof(value));
    }
    ASSERT_TRUE(file.good());
  }

  void writeParquet() {
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(builder.Finish(&values).ok());
    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("id", arrow::int64())}), {values});
    auto result = arrow::io::FileOutputStream::Open(directory_ + "/base");
    ASSERT_TRUE(result.ok()) << result.status();
    auto sink = std::move(result).ValueOrDie();
    ASSERT_TRUE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                          sink, 2).ok());
    ASSERT_TRUE(sink->Close().ok());
  }

  std::string directory_;
};

TEST_F(DataSourceTest, VecsDeliversEveryRow) {
  writeVecs();
  auto ds = dataset(DataSetFormat::FVECS_FORMAT);
  size_t rows = 0;
  VecsDataSource<float> source(&ds, 1, 1, [&](VecsBlock *block) {
    rows += block->batch_size_;
    return true;
  });
  source.start();
  EXPECT_NO_THROW(source.wait_for_finish());
  EXPECT_EQ(rows, 2);
}

TEST_F(DataSourceTest, VecsPropagatesCallbackFailureAfterAllTasksFinish) {
  writeVecs();
  auto ds = dataset(DataSetFormat::FVECS_FORMAT);
  size_t calls = 0;
  VecsDataSource<float> source(&ds, 1, 1, [&](VecsBlock *) {
    return ++calls != 1;
  });
  source.start();
  EXPECT_THROW(source.wait_for_finish(), std::runtime_error);
  EXPECT_EQ(calls, 2);
}

TEST_F(DataSourceTest, VecsPropagatesCallbackException) {
  writeVecs();
  auto ds = dataset(DataSetFormat::FVECS_FORMAT);
  VecsDataSource<float> source(&ds, 1, 1, [](VecsBlock *) -> bool {
    throw std::runtime_error("conversion failed");
  });
  source.start();
  EXPECT_THROW(source.wait_for_finish(), std::runtime_error);
}

TEST_F(DataSourceTest, VecsPropagatesReadFailure) {
  writeVecs();
  auto ds = dataset(DataSetFormat::FVECS_FORMAT);
  VecsDataSource<float> source(&ds, 1, 1, [&](VecsBlock *) {
    // The next queued read reaches EOF after this first block succeeds.
    std::filesystem::resize_file(directory_ + "/base", 0);
    return true;
  });
  source.start();
  EXPECT_THROW(source.wait_for_finish(), std::runtime_error);
}

TEST_F(DataSourceTest, VecsRejectsTruncatedFile) {
  writeVecs();
  auto ds = dataset(DataSetFormat::FVECS_FORMAT, 3);
  VecsDataSource<float> source(&ds, 1, 1, [](VecsBlock *) { return true; });
  EXPECT_THROW(source.start(), std::runtime_error);
  EXPECT_NO_THROW(source.wait_for_finish());
}

TEST_F(DataSourceTest, ParquetDeliversEveryRow) {
  writeParquet();
  auto ds = dataset(DataSetFormat::PARQUET_FORMAT);
  size_t rows = 0;
  ParquetDataSource source(&ds, 1, 1, [&](auto &batch, const DataSet *) {
    rows += batch->num_rows();
    return true;
  });
  source.start();
  EXPECT_NO_THROW(source.wait_for_finish());
  EXPECT_EQ(rows, 2);
}

TEST_F(DataSourceTest, ParquetPropagatesMissingFile) {
  auto ds = dataset(DataSetFormat::PARQUET_FORMAT);
  ParquetDataSource source(&ds, 1, 1, [](auto &, const DataSet *) { return true; });
  source.start();
  EXPECT_THROW(source.wait_for_finish(), std::runtime_error);
}

TEST_F(DataSourceTest, ParquetPropagatesCorruptFile) {
  writeVecs();
  auto ds = dataset(DataSetFormat::PARQUET_FORMAT);
  ParquetDataSource source(&ds, 1, 1, [](auto &, const DataSet *) { return true; });
  source.start();
  EXPECT_THROW(source.wait_for_finish(), std::runtime_error);
}

TEST_F(DataSourceTest, ParquetPropagatesCallbackFailure) {
  writeParquet();
  auto ds = dataset(DataSetFormat::PARQUET_FORMAT);
  ParquetDataSource source(&ds, 1, 1, [](auto &, const DataSet *) { return false; });
  source.start();
  EXPECT_THROW(source.wait_for_finish(), std::runtime_error);
}

TEST_F(DataSourceTest, ParquetPropagatesCallbackException) {
  writeParquet();
  auto ds = dataset(DataSetFormat::PARQUET_FORMAT);
  ParquetDataSource source(&ds, 1, 1, [](auto &, const DataSet *) -> bool {
    throw std::runtime_error("conversion failed");
  });
  source.start();
  EXPECT_THROW(source.wait_for_finish(), std::runtime_error);
}

TEST_F(DataSourceTest, ParquetRejectsIncompleteRowCount) {
  writeParquet();
  auto ds = dataset(DataSetFormat::PARQUET_FORMAT, 3);
  ParquetDataSource source(&ds, 1, 1, [](auto &, const DataSet *) { return true; });
  source.start();
  EXPECT_THROW(source.wait_for_finish(), std::runtime_error);
}

} // namespace
} // namespace pgvectorbench
