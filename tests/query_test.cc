#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <unistd.h>
#include <unordered_map>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/writer.h>
#include <spdlog/sinks/basic_file_sink.h>

#include "dataset/dataset.h"
#include "utils/client_factory.h"

namespace pgvectorbench {
void query(const DataSet *, const ClientFactory *,
           const std::unordered_map<std::string, std::string> &);
void create_index(const DataSet *, const ClientFactory *,
                  const std::unordered_map<std::string, std::string> &);
void load(const DataSet *, const ClientFactory *,
          const std::unordered_map<std::string, std::string> &);

namespace {
using Options = std::unordered_map<std::string, std::string>;
enum class Operation { Query, Index, Load };

class QueryTest : public testing::Test {
protected:
  void SetUp() override {
    auto pattern =
        (std::filesystem::temp_directory_path() / "pgvectorbench-query-XXXXXX")
            .string();
    ASSERT_NE(mkdtemp(pattern.data()), nullptr);
    directory_ = pattern;
    factory_ = ClientFactory::createBuilder().build();
  }

  void TearDown() override { std::filesystem::remove_all(directory_); }

  DataSet dataset(DataSetFormat format = DataSetFormat::PARQUET_FORMAT) {
    return DataSet(directory_, "items", format, DataSetBaseType::FLOAT,
                   DataSetMetric::L2, {}, {}, {}, "emb", 2, 0, {"queries", 2},
                   {"neighbors", 2}, 2);
  }

  template <typename Builder, typename T>
  void writeParquet(const std::string &filename,
                    const std::vector<int64_t> &ids,
                    const std::vector<std::vector<T>> &rows) {
    ASSERT_EQ(ids.size(), rows.size());
    arrow::Int64Builder id_builder;
    ASSERT_TRUE(id_builder.AppendValues(ids).ok());
    std::shared_ptr<arrow::Array> id_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    auto values = std::make_shared<Builder>();
    arrow::ListBuilder lists(arrow::default_memory_pool(), values);
    for (const auto &row : rows) {
      ASSERT_TRUE(lists.Append().ok());
      ASSERT_TRUE(values->AppendValues(row).ok());
    }
    std::shared_ptr<arrow::Array> list_array;
    ASSERT_TRUE(lists.Finish(&list_array).ok());
    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("id", arrow::int64()),
                       arrow::field("values", list_array->type())}),
        {id_array, list_array});
    auto result =
        arrow::io::FileOutputStream::Open(directory_ + "/" + filename);
    ASSERT_TRUE(result.ok()) << result.status();
    auto sink = std::move(result).ValueOrDie();
    // Separate row groups exercise coverage/duplicate tracking across batches.
    ASSERT_TRUE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                           sink, 1)
                    .ok());
    ASSERT_TRUE(sink->Close().ok());
  }

  void writeQueries(std::vector<int64_t> ids = {0, 1},
                    std::vector<std::vector<float>> rows = {{0, 0}, {1, 1}}) {
    writeParquet<arrow::FloatBuilder>("queries", ids, rows);
  }

  void writeGroundTruths(std::vector<int64_t> ids = {0, 1},
                         std::vector<std::vector<int64_t>> rows = {{0, 1},
                                                                   {1, 0}}) {
    writeParquet<arrow::Int64Builder>("neighbors", ids, rows);
  }

  template <typename T>
  void writeVecs(const std::string &name, uint32_t dimension,
                 const std::vector<std::vector<T>> &rows) {
    std::ofstream file(directory_ + "/" + name, std::ios::binary);
    for (const auto &row : rows) {
      file.write(reinterpret_cast<const char *>(&dimension), sizeof(dimension));
      file.write(reinterpret_cast<const char *>(row.data()),
                 row.size() * sizeof(T));
    }
    ASSERT_TRUE(file.good());
  }

  std::string run(const DataSet &ds, Options options = {}, int exit_code = 1,
                  Operation operation = Operation::Query) {
    options.try_emplace("thread_num", "1");
    const auto log = directory_ + "/run.log";
    EXPECT_EXIT(
        {
          alarm(15);
          spdlog::set_default_logger(
              spdlog::basic_logger_mt("query_test", log, true));
          spdlog::set_pattern("%v");
          spdlog::flush_on(spdlog::level::info);
          if (operation == Operation::Index) {
            create_index(&ds, factory_.get(), options);
          } else if (operation == Operation::Load) {
            load(&ds, factory_.get(), options);
          } else {
            query(&ds, factory_.get(), options);
          }
          std::exit(0);
        },
        testing::ExitedWithCode(exit_code), "");
    std::ifstream input(log);
    std::string output((std::istreambuf_iterator<char>(input)), {});
    if (exit_code != 0) {
      EXPECT_EQ(output.find("qps:"), std::string::npos) << output;
      EXPECT_EQ(output.find("latency(us):"), std::string::npos) << output;
      EXPECT_EQ(output.find("recall:"), std::string::npos) << output;
    }
    return output;
  }

  std::string directory_;
  std::unique_ptr<ClientFactory> factory_;
};

TEST_F(QueryTest, RejectsInvalidPositiveOptionsBeforeOpeningFiles) {
  const auto ds = dataset();
  for (const auto name : {"k1", "k2", "thread_num", "loop"}) {
    for (const auto value : {"", "0", "-1", "+1", "1x", "1.5", " 1",
                             "18446744073709551615", "18446744073709551616"}) {
      SCOPED_TRACE(std::string(name) + "=" + value);
      EXPECT_NE(run(ds, {{name, value}})
                    .find(std::string("Invalid query option ") + name),
                std::string::npos);
    }
  }
  EXPECT_NE(run(ds, {{"k1", "2"}, {"k2", "1"}}).find("Invalid query option k1"),
            std::string::npos);
  EXPECT_NE(run(ds, {{"k2", "3"}}).find("Invalid query option k2"),
            std::string::npos);
}

TEST_F(QueryTest, RejectsOverflowingQueryCountBeforeOpeningFiles) {
  const auto loop = std::to_string(std::numeric_limits<size_t>::max() / 2 + 1);
  EXPECT_NE(run(dataset(), {{"loop", loop}}).find("Invalid query option loop"),
            std::string::npos);
}

TEST_F(QueryTest, RejectsInvalidPercentagesBeforeOpeningFiles) {
  for (const auto value : {"", "nan", "inf", "-1", "101", "90x", "1e999",
                           "90,,99", "90,", ",90"}) {
    SCOPED_TRACE(value);
    EXPECT_NE(run(dataset(), {{"percentages", value}})
                  .find("Invalid query option percentages"),
              std::string::npos);
  }
}

TEST_F(QueryTest, RejectsEmptyOrMismatchedMetadata) {
  auto ds = dataset();
  ds.gt_file_.second = 1;
  EXPECT_NE(run(ds).find("counts must match"), std::string::npos);
  ds.query_file_.second = ds.gt_file_.second = 0;
  EXPECT_NE(run(ds).find("counts must match"), std::string::npos);
}

TEST_F(QueryTest, RejectsWrongVecsDimensionsAndFileSizes) {
  const auto ds = dataset(DataSetFormat::FVECS_FORMAT);
  writeVecs<float>("queries", 4096, {{0, 0}, {1, 1}});
  EXPECT_NE(run(ds).find("VECS query dimension"), std::string::npos);
  writeVecs<float>("queries", 2, {{0, 0}, {1, 1}});
  writeVecs<int32_t>("neighbors", 4096, {{0, 1}, {1, 0}});
  EXPECT_NE(run(ds).find("VECS ground-truth dimension"), std::string::npos);
  std::filesystem::resize_file(directory_ + "/neighbors", 12);
  EXPECT_NE(run(ds).find("ground-truth file size"), std::string::npos);
  std::filesystem::resize_file(directory_ + "/queries", 12);
  EXPECT_NE(run(ds).find("query file size"), std::string::npos);
}

TEST_F(QueryTest, RejectsMissingDuplicateAndOutOfRangeQueryIds) {
  writeQueries({0}, {{0, 0}});
  EXPECT_NE(run(dataset()).find("query row count"), std::string::npos);
  writeQueries({0, 0}, {{0, 0}, {1, 1}});
  EXPECT_NE(run(dataset()).find("duplicate query row id"), std::string::npos);
  for (const auto id : {-1, 2}) {
    writeQueries({0, id}, {{0, 0}, {1, 1}});
    EXPECT_NE(run(dataset()).find("out of range"), std::string::npos);
  }
  writeQueries({0, 1, 2}, {{0, 0}, {1, 1}, {2, 2}});
  EXPECT_NE(run(dataset()).find("out of range"), std::string::npos);
}

TEST_F(QueryTest, RejectsMissingDuplicateAndOutOfRangeGroundTruthIds) {
  writeQueries();
  writeGroundTruths({0}, {{0, 1}});
  EXPECT_NE(run(dataset()).find("ground-truth row count"), std::string::npos);
  writeGroundTruths({0, 0}, {{0, 1}, {1, 0}});
  EXPECT_NE(run(dataset()).find("duplicate ground-truth row id"),
            std::string::npos);
  for (const auto id : {-1, 2}) {
    writeGroundTruths({0, id});
    EXPECT_NE(run(dataset()).find("out of range"), std::string::npos);
  }
}

TEST_F(QueryTest, RejectsWrongParquetDimensionsAndNeighborTypes) {
  writeQueries({0, 1}, {{0}, {1}});
  EXPECT_NE(run(dataset()).find("embedding type, null or dimension"),
            std::string::npos);
  writeQueries();
  writeParquet<arrow::FloatBuilder>(
      "neighbors", {0, 1}, std::vector<std::vector<float>>{{0, 1}, {1, 0}});
  EXPECT_NE(run(dataset()).find("neighbors must be int64"), std::string::npos);
}

// Real PostgreSQL, with a schema-local text distance operator: no pgvector
// extension or downloaded dataset is needed to test the query pipeline.
class QueryDatabaseTest : public QueryTest {
protected:
  void SetUp() override {
    QueryTest::SetUp();
    const char *database = std::getenv("PGVECTORBENCH_TEST_DATABASE");
    if (!database) {
      GTEST_SKIP() << "Set PGVECTORBENCH_TEST_DATABASE to run database tests";
    }
    factory_ = ClientFactory::createBuilder().setDBName(database).build();
    admin_ = factory_->createClient();
    ASSERT_NE(admin_, nullptr);
    schema_ = "query_" + directory_.substr(directory_.size() - 6);
    exec("CREATE SCHEMA " + schema_);
    exec("CREATE FUNCTION " + schema_ +
         ".distance(text, text) RETURNS float8 "
         "LANGUAGE SQL IMMUTABLE AS $$ SELECT abs("
         "split_part(btrim($1, '[] '), ',', 1)::float8 - "
         "split_part(btrim($2, '[] '), ',', 1)::float8) $$");
    exec("CREATE OPERATOR " + schema_ +
         ".<-> (LEFTARG = text, RIGHTARG = text, "
         "FUNCTION = " +
         schema_ + ".distance)");
    exec("CREATE TABLE " + schema_ + ".items (id bigint, emb text)");
    exec("INSERT INTO " + schema_ + ".items VALUES (0, '[0,0]'), (1, '[1,1]')");
    if (const auto options = std::getenv("PGOPTIONS")) {
      old_options_ = options;
    }
    const auto options =
        old_options_.value_or("") + " -csearch_path=" + schema_ + ",public";
    ASSERT_EQ(setenv("PGOPTIONS", options.c_str(), 1), 0);
    changed_options_ = true;
    writeQueries();
    writeGroundTruths();
  }

  void TearDown() override {
    if (changed_options_) {
      if (old_options_) {
        setenv("PGOPTIONS", old_options_->c_str(), 1);
      } else {
        unsetenv("PGOPTIONS");
      }
    }
    if (admin_ && !schema_.empty()) {
      exec("DROP SCHEMA " + schema_ + " CASCADE");
    }
    QueryTest::TearDown();
  }

  void exec(const std::string &sql) {
    ASSERT_TRUE(admin_->executeQuery(sql.c_str(), [](PGresult *) {
      return true;
    })) << sql;
  }

  void expectRecall(const std::string &output, const std::string &recall) {
    EXPECT_NE(output.find("recall: best=" + recall + " worst=" + recall +
                          " average=" + recall),
              std::string::npos)
        << output;
    EXPECT_NE(output.find("qps:"), std::string::npos) << output;
    EXPECT_EQ(output.find("qps: inf"), std::string::npos) << output;
  }

  std::unique_ptr<Client> admin_;
  std::string schema_;
  std::optional<std::string> old_options_;
  bool changed_options_ = false;
};

TEST_F(QueryDatabaseTest, AlignsReorderedQueriesAndGroundTruthsById) {
  writeQueries({1, 0}, {{1, 1}, {0, 0}});
  expectRecall(run(dataset(), {{"k1", "1"}, {"k2", "1"}}, 0), "1");
  writeGroundTruths({1, 0}, {{1, 0}, {0, 1}});
  expectRecall(run(dataset(), {{"k1", "1"}, {"k2", "1"}}, 0), "1");
}

TEST_F(QueryDatabaseTest, AcceptsFullVecsTopKAndDifferentVectorDimension) {
  writeVecs<float>("queries", 2, {{0, 0}, {1, 1}});
  writeVecs<int32_t>("neighbors", 2, {{0, 1}, {1, 0}});
  expectRecall(run(dataset(DataSetFormat::FVECS_FORMAT), {}, 0), "1");
  writeVecs<float>("queries", 3, {{0, 0, 0}, {1, 1, 1}});
  auto ds = dataset(DataSetFormat::FVECS_FORMAT);
  ds.dim_ = 3;
  expectRecall(run(ds, {}, 0), "1");
}

TEST_F(QueryDatabaseTest, RepeatedQueriesAcrossWorkersKeepCorrectRecall) {
  expectRecall(run(dataset(),
                   {{"loop", "40"},
                    {"thread_num", "4"},
                    {"k1", "1"},
                    {"k2", "1"},
                    {"percentages", "0,50,100"}},
                   0),
               "1");
}

TEST_F(QueryDatabaseTest, HandlesShortAndEmptyResultsWithoutPaddingIds) {
  exec("DELETE FROM " + schema_ + ".items WHERE id = 1");
  expectRecall(run(dataset(), {}, 0), "0.5");
  exec("DELETE FROM " + schema_ + ".items");
  expectRecall(run(dataset(), {}, 0), "0");
}

TEST_F(QueryDatabaseTest, AcceptsInt64IdsAndComparesWithoutOverflow) {
  exec("UPDATE " + schema_ + ".items SET id = 2147483648 + id");
  writeGroundTruths(
      {0, 1}, {{2147483648LL, 2147483649LL}, {2147483649LL, 2147483648LL}});
  expectRecall(run(dataset(), {{"k1", "1"}, {"k2", "1"}}, 0), "1");
  exec("UPDATE " + schema_ + ".items SET id = -9223372036854775808");
  writeGroundTruths({0, 1}, {{INT64_MAX, 0}, {INT64_MAX, 0}});
  expectRecall(run(dataset(), {{"k1", "1"}, {"k2", "1"}}, 0), "0");
}

TEST_F(QueryDatabaseTest, RejectsNullMalformedAndOverflowingResultIds) {
  exec("ALTER TABLE " + schema_ + ".items ALTER COLUMN id TYPE text");
  for (const auto id : {"NULL", "'1x'", "'9223372036854775808'"}) {
    exec("UPDATE " + schema_ + ".items SET id = " + id);
    EXPECT_NE(run(dataset()).find("non-null int64"), std::string::npos);
  }
}

TEST_F(QueryDatabaseTest, SqlFailureSuppressesAllStatistics) {
  EXPECT_NE(
      run(dataset(),
          {{"table_name", "missing_table"}, {"loop", "3"}, {"thread_num", "2"}})
          .find("no statistics will be reported"),
      std::string::npos);
}

TEST_F(QueryDatabaseTest, QuerySetFailureSuppressesAllStatistics) {
  // This built-in setting rejects zero even without the pgvector extension.
  EXPECT_NE(run(dataset(), {{"hnsw.ef_search", "1; SET work_mem = 0"},
                            {"thread_num", "2"}})
                .find("no statistics will be reported"),
            std::string::npos);
}

TEST_F(QueryDatabaseTest, IndexSetFailureStopsBeforeCreateIndex) {
  // Deliberately omit index_type: reaching that validation means SET did not
  // stop execution.
  for (const auto &options :
       {Options{{"maintenance_work_mem", "invalid"}},
        Options{{"max_parallel_maintenance_workers", "-1"}}}) {
    const auto output = run(dataset(), options, 1, Operation::Index);
    EXPECT_NE(output.find("failed to execute: SET"), std::string::npos)
        << output;
    EXPECT_EQ(output.find("type of index must be explicitly specified"),
              std::string::npos)
        << output;
    EXPECT_EQ(output.find("CREATE INDEX"), std::string::npos) << output;
  }
}

TEST_F(QueryDatabaseTest, CopiesParquetFloatAndDoubleEmbeddings) {
  auto ds = dataset();
  ds.base_files_ = {{"base", 2}};
  ds.total_cnt_ = 2;
  const Options options{{"batch_size", "1"}, {"client_num", "1"}};
  for (const auto type : {DataSetBaseType::FLOAT, DataSetBaseType::DOUBLE}) {
    ds.base_type_ = type;
    exec("TRUNCATE " + schema_ + ".items");
    if (type == DataSetBaseType::FLOAT) {
      writeParquet<arrow::FloatBuilder>(
          "base", {0, 1}, std::vector<std::vector<float>>{{0, 0}, {1, 1}});
    } else {
      writeParquet<arrow::DoubleBuilder>(
          "base", {0, 1}, std::vector<std::vector<double>>{{0, 0}, {1, 1}});
      writeParquet<arrow::DoubleBuilder>(
          "queries", {0, 1}, std::vector<std::vector<double>>{{0, 0}, {1, 1}});
    }
    run(ds, options, 0, Operation::Load);
    expectRecall(run(ds, {{"k1", "1"}, {"k2", "1"}}, 0), "1");
  }
}

TEST_F(QueryDatabaseTest, RejectsMalformedEmbeddingsDuringLoad) {
  auto ds = dataset();
  ds.base_files_ = {{"base", 2}};
  ds.total_cnt_ = 2;
  const Options options{
      {"batch_size", "1"}, {"client_num", "1"}, {"queue_capacity", "1"}};
  writeParquet<arrow::FloatBuilder>(
      "base", {0, 1}, std::vector<std::vector<float>>{{0}, {1, 1}});
  EXPECT_NE(run(ds, options, 1, Operation::Load)
                .find("embedding type, null or dimension"),
            std::string::npos);
  ds.format_ = DataSetFormat::FVECS_FORMAT;
  writeVecs<float>("base", 4096, {{0, 0}, {1, 1}});
  EXPECT_NE(run(ds, options, 1, Operation::Load).find("VECS vector dimension"),
            std::string::npos);
}

TEST_F(QueryDatabaseTest, CopiesAndQueriesByteVectorsAsNumbers) {
  auto ds = dataset(DataSetFormat::BVECS_FORMAT);
  ds.base_type_ = DataSetBaseType::BYTE;
  ds.base_files_ = {{"base", 2}};
  ds.total_cnt_ = 2;
  exec("TRUNCATE " + schema_ + ".items");
  writeVecs<uint8_t>("base", 2, {{0, 0}, {255, 255}});
  writeVecs<uint8_t>("queries", 2, {{0, 0}, {255, 255}});
  writeVecs<int32_t>("neighbors", 2, {{0, 1}, {1, 0}});
  // Two byte-vector rows in one buffer also exercise unaligned row headers.
  run(ds, {{"batch_size", "2"}, {"client_num", "1"}}, 0, Operation::Load);
  expectRecall(run(ds, {{"k1", "1"}, {"k2", "1"}}, 0), "1");
}

} // namespace
} // namespace pgvectorbench
