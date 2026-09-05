#include <cstdint>
#include <memory>
#include <utility>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

int main() {
  arrow::Int64Builder builder;
  if (!builder.AppendValues({10, 20, 30}).ok()) {
    return 1;
  }

  std::shared_ptr<arrow::Array> values;
  if (!builder.Finish(&values).ok()) {
    return 1;
  }

  auto table = arrow::Table::Make(
      arrow::schema({arrow::field("id", arrow::int64())}), {values});
  auto sink_result = arrow::io::BufferOutputStream::Create();
  if (!sink_result.ok()) {
    return 1;
  }
  auto sink = std::move(sink_result).ValueOrDie();

  auto properties = parquet::WriterProperties::Builder()
                        .compression(parquet::Compression::ZSTD)
                        ->build();
  if (!parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink,
                                  table->num_rows(), properties)
           .ok()) {
    return 1;
  }

  auto buffer_result = sink->Finish();
  if (!buffer_result.ok()) {
    return 1;
  }
  auto source = std::make_shared<arrow::io::BufferReader>(
      std::move(buffer_result).ValueOrDie());
  auto reader_result =
      parquet::arrow::OpenFile(source, arrow::default_memory_pool());
  if (!reader_result.ok()) {
    return 1;
  }
  auto reader = std::move(reader_result).ValueOrDie();

  auto metadata = reader->parquet_reader()->metadata();
  if (metadata->num_row_groups() != 1 ||
      metadata->RowGroup(0)->ColumnChunk(0)->compression() !=
          parquet::Compression::ZSTD) {
    return 1;
  }

  auto actual_result = reader->ReadTable();
  if (!actual_result.ok()) {
    return 1;
  }

  auto actual = std::move(actual_result).ValueOrDie();
  return actual->Equals(*table) ? 0 : 1;
}
