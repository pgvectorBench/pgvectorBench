#include <gtest/gtest.h>

#include "dataset/dataset.h"
#include "dataset/parquet_embedding.h"

namespace pgvectorbench {
namespace {

TEST(DataSetMetadataTest, DefaultPathsAndVectorDimensionsAreConsistent) {
  for (const auto name :
       {"siftsmall", "sift", "gist", "glove", "crawl", "deep1B",
        "cohere_small_100k", "cohere_medium_1m", "cohere_large_10m",
        "openai_small_50k", "openai_medium_500k", "openai_large_5m",
        "laion_large_100m"}) {
    SCOPED_TRACE(name);
    const auto ds = getDataSet(name);
    ASSERT_NE(ds, nullptr);
    ASSERT_FALSE(ds->location_.empty());
    EXPECT_EQ(ds->location_.back(), '/');
    for (const auto &[field, type] : ds->fields_) {
      if (field == ds->vector_field_) {
        EXPECT_EQ(type, "vector(" + std::to_string(ds->dim_) + ")");
      }
    }
    if (ds->format_ == DataSetFormat::PARQUET_FORMAT &&
        ds->name_ != "laion_large_100m") {
      for (const auto suffix : {"_filter1", "_filter99"}) {
        const auto filtered = getDataSet(std::string(name) + suffix);
        ASSERT_NE(filtered, nullptr);
        EXPECT_EQ(filtered->location_, ds->location_);
        EXPECT_EQ(filtered->fields_, ds->fields_);
      }
    }
  }
  EXPECT_EQ(getDataSet("crawl")->location_, "/opt/datasets/vecs/crawl/");
  EXPECT_EQ(getDataSet("cohere_small_100k")->location_ + "train.parquet",
            "/opt/datasets/parquet/cohere_small_100k/train.parquet");
  EXPECT_EQ(getDataSet("laion_large_100m")->dim_, 768);
}

TEST(DataSetMetadataTest, NormalizesExplicitLocationsWithoutDuplicateSlashes) {
  auto ds = *getDataSet("siftsmall");
  ds.set_location("/tmp/data");
  EXPECT_EQ(ds.location_, "/tmp/data/");
  ds.set_location("/tmp/data/");
  EXPECT_EQ(ds.location_, "/tmp/data/");
  ds.set_location("");
  EXPECT_EQ(ds.location_, "");
}

template <typename T> class ParquetEmbeddingTest : public testing::Test {};
using EmbeddingTypes = testing::Types<float, double>;
TYPED_TEST_SUITE(ParquetEmbeddingTest, EmbeddingTypes);

TYPED_TEST(ParquetEmbeddingTest, RespectsSlicedListOffsets) {
  using Builder = std::conditional_t<std::is_same_v<TypeParam, float>,
                                     arrow::FloatBuilder, arrow::DoubleBuilder>;
  auto values = std::make_shared<Builder>();
  arrow::ListBuilder lists(arrow::default_memory_pool(), values);
  // A preceding short row prevents fixed-stride indexing from accidentally
  // working.
  ASSERT_TRUE(lists.Append().ok());
  ASSERT_TRUE(values->AppendValues({10}).ok());
  ASSERT_TRUE(lists.Append().ok());
  ASSERT_TRUE(values->AppendValues({20, 30}).ok());
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(lists.Finish(&array).ok());
  EXPECT_EQ(formatParquetEmbedding<TypeParam>(*array, 1, 2), "[2E1,3E1]");
  const auto sliced = array->Slice(1, 1);
  EXPECT_EQ(formatParquetEmbedding<TypeParam>(*sliced, 0, 2), "[2E1,3E1]");
  EXPECT_THROW(formatParquetEmbedding<TypeParam>(*array, 0, 2),
               std::runtime_error);
}

TYPED_TEST(ParquetEmbeddingTest, SupportsLargeListsAndRejectsNulls) {
  using Builder = std::conditional_t<std::is_same_v<TypeParam, float>,
                                     arrow::FloatBuilder, arrow::DoubleBuilder>;
  auto values = std::make_shared<Builder>();
  arrow::LargeListBuilder lists(arrow::default_memory_pool(), values);
  ASSERT_TRUE(lists.Append().ok());
  ASSERT_TRUE(values->AppendValues({20, 30}).ok());
  ASSERT_TRUE(lists.AppendNull().ok());
  ASSERT_TRUE(lists.Append().ok());
  ASSERT_TRUE(values->Append(40).ok());
  ASSERT_TRUE(values->AppendNull().ok());
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(lists.Finish(&array).ok());
  EXPECT_EQ(formatParquetEmbedding<TypeParam>(*array, 0, 2), "[2E1,3E1]");
  EXPECT_THROW(formatParquetEmbedding<TypeParam>(*array, 1, 2),
               std::runtime_error);
  EXPECT_THROW(formatParquetEmbedding<TypeParam>(*array, 2, 2),
               std::runtime_error);
}

TEST(ParquetEmbeddingSchemaTest, RejectsWrongColumnAndValueTypes) {
  arrow::Int64Builder ids;
  ASSERT_TRUE(ids.Append(0).ok());
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(ids.Finish(&array).ok());
  EXPECT_THROW(formatParquetEmbedding<float>(*array, 0, 1), std::runtime_error);
  auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("id", arrow::int64())}), 1, {array});
  EXPECT_THROW(parquetRowIds(*batch), std::runtime_error);
  auto values = std::make_shared<arrow::Int64Builder>();
  arrow::ListBuilder lists(arrow::default_memory_pool(), values);
  ASSERT_TRUE(lists.Append().ok());
  ASSERT_TRUE(values->Append(1).ok());
  std::shared_ptr<arrow::Array> list;
  ASSERT_TRUE(lists.Finish(&list).ok());
  EXPECT_THROW(formatParquetEmbedding<float>(*list, 0, 1), std::runtime_error);
}

} // namespace
} // namespace pgvectorbench

#include <filesystem>
#include <fstream>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include "dataset/vector_config.h"

namespace pgvectorbench {
TEST(VectorConfigTest, CompleteAccessMethodTypeAndDistanceMatrix) {
  for (auto type : {VectorType::VECTOR, VectorType::HALFVEC, VectorType::BIT, VectorType::SPARSEVEC}) {
    for (auto metric : {DataSetMetric::L1, DataSetMetric::L2, DataSetMetric::IP,
                        DataSetMetric::COSINE, DataSetMetric::HAMMING, DataSetMetric::JACCARD}) {
      for (std::string method : {"hnsw", "ivfflat"}) {
        SCOPED_TRACE(typeName(type) + "/" + distanceName(metric) + "/" + method);
        auto ds = *getDataSet("siftsmall");
        ds.source_type_ = ds.storage_type_ = type;
        ds.metric_ = metric;
        const bool binary = metric == DataSetMetric::HAMMING || metric == DataSetMetric::JACCARD;
        const bool valid = (type == VectorType::BIT) == binary &&
            !(method == "ivfflat" && (type == VectorType::SPARSEVEC || metric == DataSetMetric::L1 || metric == DataSetMetric::JACCARD));
        if (valid) EXPECT_NO_THROW(validateIndexConfig(ds, method));
        else EXPECT_THROW(validateIndexConfig(ds, method), std::runtime_error);
      }
    }
  }
}

TEST(VectorConfigTest, StorageAndIndexDimensionLimitsDiffer) {
  auto ds = *getDataSet("siftsmall");
  ds.dim_ = 3000;
  EXPECT_NO_THROW(configureVectors(ds, VectorType::VECTOR, IndexRepresentation::NATIVE));
  EXPECT_THROW(validateIndexConfig(ds, "hnsw"), std::runtime_error);
  configureVectors(ds, VectorType::VECTOR, IndexRepresentation::HALFVEC);
  EXPECT_NO_THROW(validateIndexConfig(ds, "hnsw"));
  ds.dim_ = 4001;
  EXPECT_THROW(validateIndexConfig(ds, "ivfflat"), std::runtime_error);
  configureVectors(ds, VectorType::VECTOR, IndexRepresentation::BINARY);
  EXPECT_NO_THROW(validateIndexConfig(ds, "ivfflat"));
  EXPECT_THROW(configureVectors(ds, VectorType::BIT, IndexRepresentation::NATIVE), std::runtime_error);
}

TEST(VectorConfigTest, BinaryCandidateSelectionPreservesEvaluationMetricAndFilters) {
  auto ds = *getDataSet("cohere_small_100k_filter99");
  configureVectors(ds, VectorType::VECTOR, IndexRepresentation::BINARY);
  EXPECT_EQ(searchMetric(ds), DataSetMetric::HAMMING);
  EXPECT_EQ(ds.metric_, DataSetMetric::COSINE);
  EXPECT_EQ(indexOpclass(ds), "bit_hamming_ops");
  const auto sql = searchSql(ds, "items", "[1,2]", 10, true, 200);
  EXPECT_NE(sql.find("WITH candidates AS MATERIALIZED"), std::string::npos);
  EXPECT_NE(sql.find("WHERE id>=99000"), std::string::npos);
  EXPECT_NE(sql.find(" <~> "), std::string::npos);
  EXPECT_NE(sql.find("LIMIT 200"), std::string::npos);
  EXPECT_NE(sql.find(" <=> "), std::string::npos);
  EXPECT_TRUE(sql.ends_with("LIMIT 10;"));
  EXPECT_THROW(searchSql(ds, "items", "[1,2]", 10, true, 9), std::runtime_error);
}

TEST(VectorConfigTest, ValidatesNativeBitsIncludingSlicedArrays) {
  auto ds = *getDataSet("siftsmall");
  ds.dim_ = 3;
  ds.source_type_ = ds.storage_type_ = VectorType::BIT;
  arrow::StringBuilder builder;
  ASSERT_TRUE(builder.AppendValues({"010", "101", " 01", "01"}).ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());
  EXPECT_EQ(formatTypedEmbedding<float>(*array->Slice(1), 0, ds), "101");
  for (int i : {2,3,4}) EXPECT_THROW(formatTypedEmbedding<float>(*array, i, ds), std::runtime_error);
}

TEST(VectorConfigTest, SparseOffsetsOneBasedSyntaxAndInvalidEntries) {
  auto ds = *getDataSet("siftsmall");
  ds.dim_ = 30000;
  ds.source_type_ = ds.storage_type_ = VectorType::SPARSEVEC;
  auto indices = std::make_shared<arrow::Int32Builder>();
  auto values = std::make_shared<arrow::FloatBuilder>();
  arrow::ListBuilder il(arrow::default_memory_pool(), indices), vl(arrow::default_memory_pool(), values);
  for (auto row : std::vector<std::vector<int32_t>>{{0}, {2, 29999}, {2,2}, {30000}}) {
    ASSERT_TRUE(il.Append().ok()); ASSERT_TRUE(vl.Append().ok());
    ASSERT_TRUE(indices->AppendValues(row).ok());
    for (auto ignored : row) ASSERT_TRUE(values->Append(0.5).ok());
  }
  std::shared_ptr<arrow::Array> ia, va;
  ASSERT_TRUE(il.Finish(&ia).ok()); ASSERT_TRUE(vl.Finish(&va).ok());
  auto array = arrow::StructArray::Make({ia, va}, std::vector<std::string>{"indices", "values"}).ValueOrDie();
  EXPECT_EQ(formatTypedEmbedding<float>(*array->Slice(1), 0, ds), "{3:5E-1,30000:5E-1}/30000");
  EXPECT_THROW(formatTypedEmbedding<float>(*array, 2, ds), std::runtime_error);
  EXPECT_THROW(formatTypedEmbedding<float>(*array, 3, ds), std::runtime_error);
}

TEST(DatasetConfigTest, ResolvesPathsAndRejectsInvalidMetadata) {
  auto pattern = (std::filesystem::temp_directory_path() / "pgvectorbench-config-XXXXXX").string();
  ASSERT_NE(mkdtemp(pattern.data()), nullptr);
  const auto file = std::filesystem::path(pattern) / "dataset.json";
  nlohmann::json j = {{"schema_version", 1}, {"name", "custom_bits"}, {"vector_type", "bit"},
    {"metric", "hamming"}, {"dimensions", 256}, {"root", "data"},
    {"base_files", {{{"path", "train.parquet"}, {"rows", 100}}}},
    {"query_file", {{"path", "test.parquet"}, {"rows", 10}}},
    {"ground_truth", {{"path", "neighbors.parquet"}, {"rows", 10}, {"neighbors", 10}}}};
  auto write = [&](const auto &value) { std::ofstream(file) << value.dump(); };
  write(j);
  const auto ds = loadDataSetConfig(file.string());
  EXPECT_EQ(ds.location_, pattern + "/data/");
  EXPECT_EQ(ds.storage_type_, VectorType::BIT);
  EXPECT_EQ(ds.fields_[1].second, "bit(256)");
  for (const auto &field : {"dimensions", "schema_version", "name"}) {
    auto bad = j; bad.erase(field); write(bad);
    EXPECT_THROW(loadDataSetConfig(file.string()), std::exception);
  }
  for (auto dimension : {-1, 0, 64001}) {
    auto bad = j; bad["dimensions"] = dimension; write(bad);
    EXPECT_THROW(loadDataSetConfig(file.string()), std::exception);
  }
  auto bad = j; bad["base_files"][0]["path"] = "../escape"; write(bad);
  EXPECT_THROW(loadDataSetConfig(file.string()), std::exception);
  std::filesystem::remove_all(pattern);
}
} // namespace pgvectorbench
