#pragma once

#include <spdlog/spdlog.h>

#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include "dataset/dataset.h"
#include "utils/file_reader.h"
#include "utils/thread_pool.h"

namespace pgvectorbench {

class DataSource {
public:
  DataSource(const DataSet *dataset, size_t batch_size, size_t thread_num)
      : dataset_(dataset), batch_size_(batch_size), thread_num_(thread_num) {
    assert(batch_size_ > 0);
    assert(thread_num > 0);

    thread_pool_ = std::make_unique<ThreadPool>(thread_num);
  }

  virtual ~DataSource() = default;

  virtual void start() = 0;

  virtual void wait_for_finish() { thread_pool_->wait_all_tasks_finished(); }

protected:
  const DataSet *dataset_;
  size_t batch_size_;
  size_t thread_num_;

  std::unique_ptr<ThreadPool> thread_pool_;
};

template <typename DataType> class VecsDataSource : public DataSource {
public:
  VecsDataSource(const DataSet *dataset, size_t batch_size, size_t thread_num,
                 std::function<bool(VecsBlock *block)> const &convert)
      : DataSource(dataset, batch_size, thread_num), convert_(convert) {}

  ~VecsDataSource() override = default;

  void start() override {

    // size of dimension(which is uint32_t) + size of vector
    size_t rowsize = (sizeof(uint32_t) + dataset_->dim_ * sizeof(DataType));
    size_t step = rowsize * batch_size_;
    size_t buffersize = step;
    size_t total_row = 0; // accumuted row count for all base files

    // pre alloc some buffer, each thread use one as read buffer
    for (auto i = 0; i < thread_num_; i++) {
      buffers_.push_back(std::string(buffersize, ' '));
    }

    for (const auto &base_file : dataset_->base_files_) {
      auto file_path = dataset_->location_ + base_file.first;
      std::shared_ptr<util::FileReader> reader =
          std::make_shared<util::FileReader>(file_path);
      reader->open();
      readers.push_back(reader);

      size_t begin = 0;
      size_t filesize = reader->filesize();
      assert(filesize == base_file.second * rowsize);
      while (begin + step < filesize) {

        // NB: pass reader pointer to lamda here
        thread_pool_->enqueue([&, begin, step, total_row, rd = reader.get()]() {
          auto thread_id_ = get_thread_id();
          assert(thread_id_ < thread_num_);

          SPDLOG_DEBUG("read {} begin: {}, step {}, thread_id {}",
                       blocks.fetch_add(1), begin, step, thread_id_);
          char *buffer = buffers_[thread_id_].data();
          rd->read(buffer, step, begin);

          VecsBlock block(buffer, total_row, batch_size_, dataset_);
          if (!convert_(&block)) {
            failed_block_num.fetch_add(1);
            SPDLOG_ERROR("bad convertion begin pos: {}, length: {}", begin,
                         step);
          }
        });
        begin += step;
        total_row += batch_size_;
      }

      // last block
      assert(begin < filesize);
      // NB: pass reader pointer to lamda here
      thread_pool_->enqueue([&, begin, step = filesize - begin, total_row,
                             rowsize, rd = reader.get()]() {
        auto thread_id_ = get_thread_id();
        assert(thread_id_ < thread_num_);

        SPDLOG_DEBUG("read {} begin: {}, step {}", blocks.fetch_add(1), begin,
                     step);
        char *buffer = buffers_[thread_id_].data();
        rd->read(buffer, step, begin);
        VecsBlock block(buffer, total_row, step / rowsize, dataset_);
        if (!convert_(&block)) {
          failed_block_num.fetch_add(1);
          SPDLOG_ERROR("bad convertion begin pos: {}, length: {}", begin, step);
        }
      });
      total_row += (filesize - begin) / rowsize;
    }

    assert(total_row == dataset_->total_cnt_);
  }

private:
  std::atomic<int> blocks{0};
  std::atomic<int> cnt{0};
  int get_thread_id() {
    thread_local int thread_id{cnt++};
    return thread_id;
  }

  // record how many blocks failed due to bad convertion
  std::atomic<size_t> failed_block_num{0};

  std::vector<std::string> buffers_;
  std::function<bool(VecsBlock *block)> convert_;
  std::vector<std::shared_ptr<util::FileReader>> readers;
};

class ParquetDataSource : public DataSource {
public:
  ParquetDataSource(const DataSet *dataset, size_t batch_size,
                    size_t thread_num,
                    std::function<bool(std::shared_ptr<arrow::RecordBatch> &,
                                       const DataSet *)> const &convert)
      : DataSource(dataset, batch_size, thread_num), convert_(convert) {}

  ~ParquetDataSource() override = default;

  void start() override {
    // // since each base file has only 1 row group, there seems no way to split
    // // the base file further.
    // for (const auto &base_file : dataset_->base_files_) {
    //   auto file_path = dataset_->location_ + base_file.first;
    //   std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
    //       parquet::ParquetFileReader::OpenFile(file_path, false);
    //   // Get the file MetaData
    //   std::shared_ptr<parquet::FileMetaData> file_metadata =
    //       parquet_reader->metadata();
    //   int num_row_groups = file_metadata->num_row_groups();
    //   SPDLOG_INFO("{} has {} row groups", file_path, num_row_groups);
    // }

    for (const auto &base_file : dataset_->base_files_) {
      auto file_path = dataset_->location_ + base_file.first;

      thread_pool_->enqueue([&, file_path, batch_size = batch_size_,
                             file_row_num = base_file.second]() {
        arrow::MemoryPool *pool = arrow::default_memory_pool();
        // general Parquet reader settings
        auto reader_properties = parquet::ReaderProperties(pool);
        reader_properties.set_buffer_size(2048 * 1024);
        reader_properties.enable_buffered_stream();

        // Arrow-specific Parquet reader settings
        auto arrow_reader_props = parquet::ArrowReaderProperties();
        arrow_reader_props.set_batch_size(batch_size);

        parquet::arrow::FileReaderBuilder reader_builder;
        auto status = reader_builder.OpenFile(file_path, /*memory_map*/ false,
                                              reader_properties);
        if (!status.ok()) {
          SPDLOG_ERROR("open file failed: {}", status.ToString());
          return;
        }
        reader_builder.memory_pool(pool);
        reader_builder.properties(arrow_reader_props);

        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        status = reader_builder.Build(&arrow_reader);
        if (!status.ok()) {
          SPDLOG_ERROR("build arrow reader failed: {}", status.ToString());
          return;
        }

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        status = arrow_reader->GetRecordBatchReader(&rb_reader);
        if (!status.ok()) {
          SPDLOG_ERROR("get record batch reader failed: {}", status.ToString());
          return;
        }

        size_t total_row = 0;
        std::shared_ptr<arrow::RecordBatch> recordBatch;
        do {
          status = rb_reader->ReadNext(&recordBatch);
          if (!status.ok()) {
            SPDLOG_ERROR("read next batch failed: {}", status.ToString());
            break;
          }
          if (recordBatch) {
            if (!convert_(recordBatch, dataset_)) {
              SPDLOG_ERROR("failed to handle recordBatch after handle {} "
                           "number of rows");
            }
            total_row += recordBatch->num_rows();
          }

        } while (recordBatch);

        assert(total_row = file_row_num);
      });
    }
  }

private:
  std::function<bool(std::shared_ptr<arrow::RecordBatch> &, const DataSet *)>
      convert_;
};

} // namespace pgvectorbench
