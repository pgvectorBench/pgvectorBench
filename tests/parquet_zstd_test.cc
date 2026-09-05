#include <cstdint>
#include <memory>
#include <utility>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

TEST(ParquetZstdTest, RoundTripsTableWithZstdCompression) {
  arrow::Int64Builder builder;
  ASSERT_TRUE(builder.AppendValues({10, 20, 30}).ok());

  std::shared_ptr<arrow::Array> values;
  ASSERT_TRUE(builder.Finish(&values).ok());

  auto table = arrow::Table::Make(
      arrow::schema({arrow::field("id", arrow::int64())}), {values});
  auto sink_result = arrow::io::BufferOutputStream::Create();
  ASSERT_TRUE(sink_result.ok()) << sink_result.status();
  auto sink = std::move(sink_result).ValueOrDie();

  auto properties = parquet::WriterProperties::Builder()
                        .compression(parquet::Compression::ZSTD)
                        ->build();
  auto write_status = parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), sink, table->num_rows(), properties);
  ASSERT_TRUE(write_status.ok()) << write_status;

  auto buffer_result = sink->Finish();
  ASSERT_TRUE(buffer_result.ok()) << buffer_result.status();
  auto source = std::make_shared<arrow::io::BufferReader>(
      std::move(buffer_result).ValueOrDie());
  auto reader_result =
      parquet::arrow::OpenFile(source, arrow::default_memory_pool());
  ASSERT_TRUE(reader_result.ok()) << reader_result.status();
  auto reader = std::move(reader_result).ValueOrDie();

  auto metadata = reader->parquet_reader()->metadata();
  ASSERT_EQ(metadata->num_row_groups(), 1);
  EXPECT_EQ(metadata->RowGroup(0)->ColumnChunk(0)->compression(),
            parquet::Compression::ZSTD);

  auto actual_result = reader->ReadTable();
  ASSERT_TRUE(actual_result.ok()) << actual_result.status();

  auto actual = std::move(actual_result).ValueOrDie();
  EXPECT_TRUE(actual->Equals(*table));
}
