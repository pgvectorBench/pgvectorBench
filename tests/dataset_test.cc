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
