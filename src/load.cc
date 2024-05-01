#include <algorithm>
#include <iomanip>
#include <memory>
#include <sstream>
#include <type_traits>
#include <unordered_set>

// third party
#include <arrow/array.h>
#include <concurrentqueue.h>
#include <lightweightsemaphore.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <ryu/ryu.h>

#include "dataset/dataset.h"
#include "dataset/datasource.h"
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
    uint32_t dim = *(uint32_t *)(block->buffer_ + rowsize * i);
    assert(dim == ds_dim);
    DataType *vecs =
        (DataType *)(block->buffer_ + rowsize * i + sizeof(uint32_t));
    if constexpr (std::is_same_v<DataType, float>) {
      for (size_t j = 0; j < dim; j++) {
        f2s_buffered(vecs[j], result);
        oss << result;
        if (j != dim - 1) {
          oss << ",";
        }
      }
    } else {
      for (size_t j = 0; j < dim; j++) {
        oss << vecs[j];
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
  auto id_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
  auto list_array =
      std::static_pointer_cast<arrow::ListArray>(batch->column(1));

  std::ostringstream oss;
  bool flag = true;
  size_t begin = 0;
  if constexpr (std::is_same_v<DataType, float>) {
    auto float_array =
        std::static_pointer_cast<arrow::FloatArray>(list_array->values());

    char result[16]; // used for converting floating point numbers to decimal

    // strings
    for (size_t i = 0; i < batch->num_rows(); i++) {
      if (flag) {
        flag = false;
      } else {
        oss << '\n';
      }
      oss << id_array->Value(i) << " | [";
      size_t end = begin + dataset->dim_;
      for (int j = begin; j < end; j++) {
        f2s_buffered(float_array->Value(j), result);
        oss << result;
        if (j != end - 1) {
          oss << ",";
        }
      }
      oss << "]";
      begin = end;
    }
  } else {
    auto double_array =
        std::static_pointer_cast<arrow::DoubleArray>(list_array->values());
    char result[40]; // used for converting double floating point numbers to
                     // decimal strings
    for (size_t i = 0; i < batch->num_rows(); i++) {
      if (flag) {
        flag = false;
      } else {
        oss << '\n';
      }
      oss << id_array->Value(i) << " | [";
      size_t end = begin + dataset->dim_;
      for (int j = begin; j < end; j++) {
        d2s_buffered(double_array->Value(j), result);
        oss << result;
        if (j != end - 1) {
          oss << ",";
        }
      }
      oss << "]";
      begin = end;
    }
  }

  return oss.str();
}

} // namespace

void load(const DataSet *dataset, const ClientFactory *cf,
          const std::unordered_map<std::string, std::string> &load_opt_map) {
  assert(dataset != nullptr);
  assert(cf != nullptr);
  size_t batch_size = default_load_batch_size;
  size_t thread_num = std::thread::hardware_concurrency() * 2;
  size_t client_num =
      std::thread::hardware_concurrency(); // number of pg client
  ssize_t queue_capacity = default_queue_capacity;

  // parse batch size
  auto bs = Util::getValueFromMap(load_opt_map, "batch_size");
  if (bs.has_value()) {
    batch_size = std::stoul(bs.value());
  }
  // parse thread num
  auto tn = Util::getValueFromMap(load_opt_map, "thread_num");
  if (tn.has_value()) {
    thread_num = std::stoul(tn.value());
  }
  // parse client num
  auto cn = Util::getValueFromMap(load_opt_map, "client_num");
  if (cn.has_value()) {
    client_num = std::stoul(cn.value());
  }

  // parse queue capacity
  auto qc = Util::getValueFromMap(load_opt_map, "queue_capacity");
  if (qc.has_value()) {
    queue_capacity = std::stol(cn.value());
  }

  auto table_name = Util::getValueFromMap(load_opt_map, "table_name");
  auto copy_table_statement = generateCopyTableStatement(dataset, table_name);

  ConcurrentQueue<std::string> sql_queue;   // lock free MPMC
  LightweightSemaphore sem(queue_capacity); // use this to limit sql_queue size

  std::atomic<bool> finished{false};
  std::unique_ptr<DataSource> datasource;
  switch (dataset->format_) {
  case DataSetFormat::FVECS_FORMAT:
    datasource.reset(new VecsDataSource<float>(
        dataset, batch_size, thread_num, [&](VecsBlock *block) -> bool {
          sem.wait();
          auto str = VecsToCopyContent<float>(block);
          if (sql_queue.enqueue(str)) {
            SPDLOG_DEBUG("enqueue start_id: {} content: {}", block->start_id_,
                         str);
            return true;
          }
          sem.signal();
          SPDLOG_ERROR("enqueue failed");
          return false;
        }));
    break;
  case DataSetFormat::BVECS_FORMAT:
    datasource.reset(new VecsDataSource<uint8_t>(
        dataset, batch_size, thread_num, [&](VecsBlock *block) -> bool {
          sem.wait();
          auto str = VecsToCopyContent<uint8_t>(block);
          if (sql_queue.enqueue(str)) {
            SPDLOG_DEBUG("enqueue start_id: {} content: {}", block->start_id_,
                         str);
            return true;
          }
          sem.signal();
          SPDLOG_ERROR("enqueue failed");
          return true;
        }));
    break;
  case DataSetFormat::PARQUET_FORMAT:
    if (dataset->base_type_ == DataSetBaseType::FLOAT) {
      datasource.reset(new ParquetDataSource(
          dataset, batch_size, thread_num,
          [&](std::shared_ptr<arrow::RecordBatch> &batch,
              const DataSet *ds) -> bool {
            sem.wait();
            auto str = RecordBatchToCopyContent<float>(batch, ds);
            if (sql_queue.enqueue(str)) {
              SPDLOG_DEBUG("enqueue content: {}", str);
              return true;
            }
            sem.signal();
            SPDLOG_ERROR("enqueue failed");
            return true;
          }));
    } else {
      assert(dataset->base_type_ == DataSetBaseType::DOUBLE);
      datasource.reset(new ParquetDataSource(
          dataset, batch_size, thread_num,
          [&](std::shared_ptr<arrow::RecordBatch> &batch,
              const DataSet *ds) -> bool {
            sem.wait();
            auto str = RecordBatchToCopyContent<double>(batch, ds);
            if (sql_queue.enqueue(str)) {
              SPDLOG_DEBUG("enqueue content: {}", str);
              return true;
            }
            sem.signal();
            SPDLOG_ERROR("enqueue failed");
            return true;
          }));
    }

    break;
  default:
    SPDLOG_ERROR("Format not supported");
    std::exit(1);
  }

  datasource->start();

  std::vector<std::thread> threads;
  for (size_t i = 0; i < client_num; i++) {
    threads.emplace_back([&]() {
      auto client = cf->createClient();
      std::string ele;
      while (!finished.load() || sem.availableApprox() != queue_capacity) {
        if (sql_queue.try_dequeue(ele)) {
          auto ret = client->copy(copy_table_statement.c_str(), ele.c_str(),
                                  ele.size(), [&](PGresult *res) -> bool {
                                    // no need to handle result
                                    return true;
                                  });
          if (!ret) {
            SPDLOG_ERROR("failed to handle copy command: {}", ele);
          }
          sem.signal();
        }
      }
    });
  }

  datasource->wait_for_finish();

  SPDLOG_DEBUG("datasouce has finished all reading");
  finished.store(true);

  for (size_t i = 0; i < client_num; i++) {
    threads.at(i).join();
  }

  SPDLOG_DEBUG("LightweightSemaphore availableApprox: {}",
               sem.availableApprox());
}

} // namespace pgvectorbench
