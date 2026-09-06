#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <type_traits>
#include <unordered_map>

// third party
#include <arrow/array.h>
#include <concurrentqueue.h>
#include <lightweightsemaphore.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <ryu/ryu.h>

#include "dataset/dataset.h"
#include "phases.h"
#include "dataset/datasource.h"
#include "dataset/parquet_embedding.h"
#include "utils/client_factory.h"
#include "utils/parser.h"
#include "utils/util.h"

namespace pgvectorbench {

using moodycamel::ConcurrentQueue;
using moodycamel::LightweightSemaphore;

namespace {

const static size_t default_load_batch_size = 100;
const static ssize_t default_queue_capacity = 64;
constexpr auto max_precision{std::numeric_limits<long double>::digits10 + 1};

std::string
generateCopyTableStatement(const DataSet *dataset,
                           const std::optional<std::string> &table_name) {
  std::ostringstream oss;
  oss << "COPY "
      << (table_name.has_value() ? table_name.value() : dataset->name_);
  oss << " FROM STDIN WITH (FORMAT CSV, DELIMITER '|')";

  std::string statement = oss.str();
  SPDLOG_DEBUG("copy table statement: {}", statement);
  return statement;
}

template <typename DataType>
std::string VecsToCopyContent(const VecsBlock *block) {
  uint32_t ds_dim = block->dataset_->dim_;
  std::ostringstream oss;
  char result[16]; // used for converting floating point numbers to decimal
                   // strings
  bool flag = true;
  size_t rowsize = (sizeof(uint32_t) + ds_dim * sizeof(DataType));

  for (size_t i = 0; i < block->batch_size_; i++) {
    if (flag) {
      flag = false;
    } else {
      oss << '\n';
    }
    oss << block->start_id_ + i << " | [";
    uint32_t dim;
    const char *row = block->buffer_ + rowsize * i;
    std::memcpy(&dim, row, sizeof(dim));
    if (dim != ds_dim) {
      throw std::runtime_error("VECS vector dimension does not match dataset");
    }
    if constexpr (std::is_same_v<DataType, float>) {
      for (size_t j = 0; j < dim; j++) {
        float value;
        std::memcpy(&value, row + sizeof(dim) + j * sizeof(value),
                    sizeof(value));
        f2s_buffered(value, result);
        oss << result;
        if (j != dim - 1) {
          oss << ",";
        }
      }
    } else {
      for (size_t j = 0; j < dim; j++) {
        oss << static_cast<unsigned int>(
            static_cast<uint8_t>(row[sizeof(dim) + j]));
        if (j != dim - 1) {
          oss << ",";
        }
      }
    }
    oss << "]";
  }

  return oss.str();
}

template <typename DataType>
std::string RecordBatchToCopyContent(std::shared_ptr<arrow::RecordBatch> &batch,
                                     const DataSet *dataset) {
  const auto &ids = parquetRowIds(*batch);
  std::ostringstream oss;
  for (int64_t i = 0; i < batch->num_rows(); ++i) {
    if (ids.IsNull(i)) {
      throw std::runtime_error("Parquet row ids must not be null");
    }
    if (i != 0) {
      oss << '\n';
    }
    oss << ids.Value(i) << "|"
        << formatTypedEmbedding<DataType>(*batch->column(1), i, *dataset);
  }
  return oss.str();
}

} // namespace

LoadResult load(const DataSet *dataset, const ClientFactory *cf,
          const std::unordered_map<std::string, std::string> &load_opt_map) {
  assert(dataset != nullptr);
  assert(cf != nullptr);
  const size_t cpu_num = std::max(1u, std::thread::hardware_concurrency());
  size_t batch_size = default_load_batch_size;
  size_t thread_num = dataset->format_ == DataSetFormat::PARQUET_FORMAT
                          ? dataset->base_files_.size()
                          : cpu_num * 2;
  size_t client_num = cpu_num; // number of pg client
  ssize_t queue_capacity = default_queue_capacity;

  auto parse_positive = [](const char *name, const std::string &value,
                           size_t maximum) {
    size_t parsed = 0;
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size() ||
        parsed == 0 || parsed > maximum) {
      SPDLOG_ERROR("Invalid load option {}={}: expected an integer from 1 to {}",
                   name, value, maximum);
      std::exit(1);
    }
    return parsed;
  };

  // parse batch size
  auto bs = Util::getValueFromMap(load_opt_map, "batch_size");
  if (bs.has_value()) {
    size_t max_batch_size = std::numeric_limits<int64_t>::max();
    if (dataset->format_ != DataSetFormat::PARQUET_FORMAT) {
      const size_t value_size = dataset->format_ == DataSetFormat::FVECS_FORMAT
                                    ? sizeof(float)
                                    : sizeof(uint8_t);
      const size_t row_size = sizeof(uint32_t) + dataset->dim_ * value_size;
      max_batch_size = std::string().max_size() / row_size;
    }
    batch_size = parse_positive("batch_size", *bs, max_batch_size);
  }
  // parse thread num used for datasource
  auto tn = Util::getValueFromMap(load_opt_map, "thread_num");
  if (tn.has_value()) {
    auto parsed_thread_num =
        parse_positive("thread_num", *tn, std::numeric_limits<int>::max());
    if (dataset->format_ == DataSetFormat::PARQUET_FORMAT) {
      if (parsed_thread_num > thread_num) {
        SPDLOG_WARN("The specified thread number {} exceeds the optimal number "
                    "for processing {} base {}, fallback to using {}",
                    parsed_thread_num, dataset->base_files_.size(),
                    dataset->base_files_.size() > 1 ? "files" : "file",
                    thread_num);
        std::exit(1);
      } else {
        thread_num = parsed_thread_num;
      }
    } else {
      thread_num = parsed_thread_num;
    }
  }
  if (thread_num == 0) {
    SPDLOG_ERROR("Cannot load a dataset without base files");
    std::exit(1);
  }

  // parse client num
  auto cn = Util::getValueFromMap(load_opt_map, "client_num");
  if (cn.has_value()) {
    client_num = parse_positive("client_num", *cn,
                                std::vector<std::thread>().max_size());
  }

  // parse queue capacity
  auto qc = Util::getValueFromMap(load_opt_map, "queue_capacity");
  if (qc.has_value()) {
    queue_capacity = parse_positive("queue_capacity", *qc,
                                    std::numeric_limits<ssize_t>::max());
  }

  auto table_name = Util::getValueFromMap(load_opt_map, "table_name");
  auto copy_table_statement = generateCopyTableStatement(dataset, table_name);

  // Validate every connection before starting producers or consumers.
  std::vector<std::unique_ptr<Client>> clients;
  for (size_t i = 0; i < client_num; i++) {
    auto client = cf->createClient();
    if (!client) {
      std::exit(1);
    }
    clients.push_back(std::move(client));
  }

  ConcurrentQueue<std::string> sql_queue;   // lock free MPMC
  LightweightSemaphore sem(queue_capacity); // use this to limit sql_queue size

  std::atomic<bool> finished{false};
  std::atomic<bool> failed{false};
  auto enqueue = [&](std::string str) -> bool {
    if (failed.load()) {
      return false;
    }
    sem.wait();
    try {
      if (sql_queue.enqueue(std::move(str))) {
        return true;
      }
    } catch (...) {
      sem.signal();
      throw;
    }
    sem.signal();
    return false;
  };
  std::unique_ptr<DataSource> datasource;
  switch (dataset->format_) {
  case DataSetFormat::FVECS_FORMAT:
    datasource.reset(new VecsDataSource<float>(
        dataset, batch_size, thread_num, [&](VecsBlock *block) -> bool {
          return enqueue(VecsToCopyContent<float>(block));
        }));
    break;
  case DataSetFormat::BVECS_FORMAT:
    datasource.reset(new VecsDataSource<uint8_t>(
        dataset, batch_size, thread_num, [&](VecsBlock *block) -> bool {
          return enqueue(VecsToCopyContent<uint8_t>(block));
        }));
    break;
  case DataSetFormat::PARQUET_FORMAT:
    if (dataset->base_type_ == DataSetBaseType::FLOAT) {
      datasource.reset(new ParquetDataSource(
          dataset, batch_size, thread_num,
          [&](std::shared_ptr<arrow::RecordBatch> &batch,
              const DataSet *ds) -> bool {
            return enqueue(RecordBatchToCopyContent<float>(batch, ds));
          }));
    } else {
      assert(dataset->base_type_ == DataSetBaseType::DOUBLE);
      datasource.reset(new ParquetDataSource(
          dataset, batch_size, thread_num,
          [&](std::shared_ptr<arrow::RecordBatch> &batch,
              const DataSet *ds) -> bool {
            return enqueue(RecordBatchToCopyContent<double>(batch, ds));
          }));
    }

    break;
  default:
    SPDLOG_ERROR("Format not supported");
    std::exit(1);
  }

  std::atomic<uint64_t> copied_rows{0};
  std::atomic<uint64_t> copied_bytes{0};
  const auto start = std::chrono::steady_clock::now();
  std::vector<std::thread> threads;
  for (size_t i = 0; i < client_num; i++) {
    threads.emplace_back([&, client = std::move(clients[i])]() {
      std::string ele;
      while (!finished.load() || sem.availableApprox() != queue_capacity) {
        if (sql_queue.try_dequeue(ele)) {
          if (!failed.load()) {
            auto ret = client->copy(copy_table_statement.c_str(), ele.c_str(),
                                   ele.size(), [&](PGresult *res) -> bool {
                                     const char *count = PQcmdTuples(res);
                                     uint64_t rows = 0;
                                     const auto end = count + std::strlen(count);
                                     const auto [parsed, error] =
                                         std::from_chars(count, end, rows);
                                     if (error != std::errc{} || parsed != end) {
                                       return false;
                                     }
                                     copied_rows.fetch_add(rows);
                                     copied_bytes.fetch_add(ele.size());
                                     return true;
                                   });
            if (!ret) {
              failed.store(true);
              SPDLOG_ERROR("failed to handle copy command");
            }
          }
          sem.signal();
        }
      }
    });
  }

  try {
    datasource->start();
  } catch (const std::exception &error) {
    failed.store(true);
    SPDLOG_ERROR("Failed to start loading dataset: {}", error.what());
  }
  // Even if start() failed after scheduling some tasks, keep consumers alive
  // until every producer finishes and releases its buffers.
  try {
    datasource->wait_for_finish();
  } catch (const std::exception &error) {
    failed.store(true);
    SPDLOG_ERROR("Failed to read or convert dataset: {}", error.what());
  }

  SPDLOG_DEBUG("datasouce has finished all reading");
  finished.store(true);

  for (size_t i = 0; i < client_num; i++) {
    threads.at(i).join();
  }

  SPDLOG_DEBUG("LightweightSemaphore availableApprox: {}",
               sem.availableApprox());
  if (failed.load()) {
    std::exit(1);
  }
  LoadResult result;
  result.table_name = table_name.value_or(dataset->name_);
  result.elapsed_seconds = std::chrono::duration<double>(
      std::chrono::steady_clock::now() - start).count();
  result.rows = copied_rows.load();
  result.copy_bytes = copied_bytes.load();
  result.rows_per_second = result.rows / std::max(result.elapsed_seconds, 1e-9);
  result.copy_bytes_per_second = result.copy_bytes / std::max(result.elapsed_seconds, 1e-9);
  SPDLOG_INFO("loaded {} rows in {} s ({} rows/s, {} COPY bytes/s)",
              result.rows, result.elapsed_seconds, result.rows_per_second,
              result.copy_bytes_per_second);
  return result;
}

} // namespace pgvectorbench
