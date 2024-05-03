#include <atomic>
#include <chrono>
#include <memory>
#include <sstream>
#include <thread>
#include <tuple>
#include <type_traits>

// third party
#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <ryu/ryu.h>

#include "dataset/dataset.h"
#include "utils/client_factory.h"
#include "utils/file_reader.h"
#include "utils/parser.h"
#include "utils/util.h"

namespace pgvectorbench {

namespace {

template <typename DataType>
std::vector<std::string>
prepareVecsQueries(const DataSet *dataset,
                   const std::optional<std::string> &table_name,
                   size_t top_k2) {
  // test query file path
  auto file_path = dataset->location_ + dataset->query_file_.first;
  std::unique_ptr<util::FileReader> reader =
      std::make_unique<util::FileReader>(file_path);
  reader->open();
  const size_t filesize = reader->filesize();
  const size_t rowsize = sizeof(uint32_t) + dataset->dim_ * sizeof(DataType);
  const size_t rowcnt = dataset->query_file_.second;
  assert(filesize == rowsize * rowcnt);

  std::vector<std::string> queries;
  queries.reserve(rowcnt);

  std::string buffer_(rowsize, ' ');
  char *buffer = buffer_.data();
  char result[16]; // used for converting floating point numbers to decimal
                   // strings

  std::ostringstream oss;
  oss << "SELECT id FROM "
      << (table_name.has_value() ? table_name.value() : dataset->name_);
  oss << " ORDER BY " << dataset->vector_field_ << " "
      << metric2operator(dataset->metric_) << " ";

  std::string sql_prefix = oss.str();

  for (size_t i = 0; i < rowcnt; i++) {
    reader->read(buffer, rowsize, rowsize * i);
    uint32_t dim = *((uint32_t *)buffer);
    assert(dim = dataset->dim_);

    oss << "'[";
    DataType *vecs = (DataType *)(buffer + sizeof(uint32_t));
    if constexpr (std::is_same_v<DataType, float>) {
      for (size_t j = 0; j < dim; j++) {
        f2s_buffered(vecs[j], result);
        oss << result;
        if (j != dim - 1) {
          oss << ',';
        }
      }
    } else {
      for (size_t j = 0; j < dim; j++) {
        oss << vecs[j];
        if (j != dim - 1) {
          oss << ',';
        }
      }
    }
    oss << "]' LIMIT " << top_k2 << ";";

    queries.push_back(oss.str());
    oss.str("");
    oss << sql_prefix;
  }

  return queries;
}

std::vector<std::vector<int64_t>> prepareVecsGroudTruths(const DataSet *dataset,
                                                         size_t top_k1) {
  assert(top_k1 <= dataset->gt_topk_);
  // ground truth file path
  auto file_path = dataset->location_ + dataset->gt_file_.first;
  std::unique_ptr<util::FileReader> reader =
      std::make_unique<util::FileReader>(file_path);
  reader->open();
  const size_t filesize = reader->filesize();
  const size_t rowsize = sizeof(uint32_t) + sizeof(int) * dataset->gt_topk_;
  const size_t rowcnt = dataset->gt_file_.second;
  assert(filesize == rowsize * rowcnt);
  std::vector<std::vector<int64_t>> gts(rowcnt, std::vector<int64_t>(top_k1));

  std::string buffer_(rowsize, ' ');
  char *buffer = buffer_.data();

  for (size_t i = 0; i < rowcnt; i++) {
    reader->read(buffer, rowsize, rowsize * i);
    uint32_t dim = *((uint32_t *)buffer);
    assert(dim = dataset->dim_);
    assert(dim > top_k1);
    int *vecs = (int *)(buffer + sizeof(uint32_t));
    for (int j = 0; j < top_k1; j++) {
      gts[i][j] = vecs[j];
    }
    std::sort(gts[i].begin(), gts[i].end());
  }

  return gts;
}

template <typename DataType>
std::vector<std::string>
prepareParquetQueries(const DataSet *dataset,
                      const std::optional<std::string> &table_name,
                      size_t top_k2) {
  // test query file path
  auto file_path = dataset->location_ + dataset->query_file_.first;
  arrow::MemoryPool *pool = arrow::default_memory_pool();
  // general Parquet reader settings
  auto reader_properties = parquet::ReaderProperties(pool);
  reader_properties.set_buffer_size(1024 * 1024);
  reader_properties.enable_buffered_stream();

  parquet::arrow::FileReaderBuilder reader_builder;
  auto status = reader_builder.OpenFile(file_path, /*memory_map*/ false);
  if (!status.ok()) {
    SPDLOG_ERROR("open file failed: {}", status.ToString());
    std::exit(1);
  }
  reader_builder.memory_pool(pool);

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  status = reader_builder.Build(&arrow_reader);
  if (!status.ok()) {
    SPDLOG_ERROR("build arrow reader failed: {}", status.ToString());
    std::exit(1);
  }

  std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
  status = arrow_reader->GetRecordBatchReader(&rb_reader);
  if (!status.ok()) {
    SPDLOG_ERROR("get record batch reader failed: {}", status.ToString());
    std::exit(1);
  }

  std::vector<std::string> queries;
  queries.reserve(dataset->query_file_.second);
  char result[40]; // used for converting floating / double floating point
                   // numbers to decimal strings

  std::ostringstream oss;
  oss << "SELECT id FROM "
      << (table_name.has_value() ? table_name.value() : dataset->name_);
  if (!dataset->filter_fields_.empty()) {
    oss << " WHERE ";
    for (const auto &filter : dataset->filter_fields_) {
      oss << std::get<0>(filter); // prologue
      oss << std::get<1>(filter); // field name
      oss << std::get<2>(filter); // operator
      oss << std::get<3>(filter); // value
      oss << std::get<4>(filter); // epilogue
    }
  }
  oss << " ORDER BY " << dataset->vector_field_ << " "
      << metric2operator(dataset->metric_) << " ";

  std::string sql_prefix = oss.str();

  std::shared_ptr<arrow::RecordBatch> recordBatch;
  do {
    status = rb_reader->ReadNext(&recordBatch);
    if (!status.ok()) {
      SPDLOG_ERROR("read next batch failed: {}", status.ToString());
      std::exit(1);
    }
    if (recordBatch) {
      auto id_array =
          std::static_pointer_cast<arrow::Int64Array>(recordBatch->column(0));
      auto list_array =
          std::static_pointer_cast<arrow::ListArray>(recordBatch->column(1));
      size_t begin = 0;
      if constexpr (std::is_same_v<DataType, float>) {
        auto float_array =
            std::static_pointer_cast<arrow::FloatArray>(list_array->values());
        for (size_t i = 0; i < recordBatch->num_rows(); i++) {
          oss << "'[";
          size_t end = begin + dataset->dim_;
          for (int j = begin; j < end; j++) {
            f2s_buffered(float_array->Value(j), result);
            oss << result;
            if (j != end - 1) {
              oss << ",";
            }
          }
          oss << "]' LIMIT " << top_k2 << ";";
          begin = end;
          queries.push_back(oss.str());
          oss.str("");
          oss << sql_prefix;
        }
      } else {
        auto double_array =
            std::static_pointer_cast<arrow::DoubleArray>(list_array->values());
        for (size_t i = 0; i < recordBatch->num_rows(); i++) {
          oss << "'[";
          size_t end = begin + dataset->dim_;
          for (int j = begin; j < end; j++) {
            d2s_buffered(double_array->Value(j), result);
            oss << result;
            if (j != end - 1) {
              oss << ",";
            }
          }
          oss << "]' LIMIT " << top_k2 << ";";
          begin = end;
          queries.push_back(oss.str());
          oss.str("");
          oss << sql_prefix;
        }
      }
    }
  } while (recordBatch);

  return queries;
}

std::vector<std::vector<int64_t>>
prepareParquetGroundTruths(const DataSet *dataset, size_t top_k1) {
  assert(top_k1 <= dataset->gt_topk_);
  // ground truth file path
  auto file_path = dataset->location_ + dataset->gt_file_.first;
  arrow::MemoryPool *pool = arrow::default_memory_pool();
  // general Parquet reader settings
  auto reader_properties = parquet::ReaderProperties(pool);
  reader_properties.set_buffer_size(1024 * 1024);
  reader_properties.enable_buffered_stream();

  parquet::arrow::FileReaderBuilder reader_builder;
  auto status = reader_builder.OpenFile(file_path, /*memory_map*/ false);
  if (!status.ok()) {
    SPDLOG_ERROR("open file failed: {}", status.ToString());
    std::exit(1);
  }
  reader_builder.memory_pool(pool);

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  status = reader_builder.Build(&arrow_reader);
  if (!status.ok()) {
    SPDLOG_ERROR("build arrow reader failed: {}", status.ToString());
    std::exit(1);
  }

  std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
  status = arrow_reader->GetRecordBatchReader(&rb_reader);
  if (!status.ok()) {
    SPDLOG_ERROR("get record batch reader failed: {}", status.ToString());
    std::exit(1);
  }

  std::vector<std::vector<int64_t>> gts(dataset->gt_file_.second,
                                        std::vector<int64_t>(top_k1));

  std::shared_ptr<arrow::RecordBatch> recordBatch;
  do {
    status = rb_reader->ReadNext(&recordBatch);
    if (!status.ok()) {
      SPDLOG_ERROR("read next batch failed: {}", status.ToString());
      std::exit(1);
    }
    if (recordBatch) {
      auto id_array =
          std::static_pointer_cast<arrow::Int64Array>(recordBatch->column(0));
      auto list_array =
          std::static_pointer_cast<arrow::ListArray>(recordBatch->column(1));
      auto int_array =
          std::static_pointer_cast<arrow::Int64Array>(list_array->values());
      for (size_t i = 0; i < recordBatch->num_rows(); i++) {
        size_t begin = list_array->value_offset(i * 2);
        size_t end = list_array->value_offset((i + 1) * 2);
        for (int j = begin; j < begin + top_k1; j++) {
          gts[id_array->Value(i)][j - begin] = int_array->Value(j);
        }
        std::sort(gts[id_array->Value(i)].begin(),
                  gts[id_array->Value(i)].end());
      }
    }
  } while (recordBatch);

  return gts;
}

template <typename T>
std::string
percentile2str(Percentile<T> p,
               const std::vector<std::pair<std::string, double>> &percentages) {
  std::ostringstream oss;
  oss << "best=" << p.best() << " worst=" << p.worst()
      << " average=" << p.average();

  for (auto it = percentages.begin(); it != percentages.end(); it++) {
    oss << " P(" << it->first << "%)=" << p(it->second);
  }

  return oss.str();
}

std::vector<std::string> generateQueryOptions(
    const std::unordered_map<std::string, std::string> &query_opt_map) {
  std::vector<std::string> sqls;

  // hnsw
  if (query_opt_map.find("hnsw.ef_search") != query_opt_map.end()) {
    sqls.push_back(fmt::format("SET hnsw.ef_search = {}",
                               query_opt_map.at("hnsw.ef_search")));
  }

  // ivfflat
  if (query_opt_map.find("ivfflat.probes") != query_opt_map.end()) {
    sqls.push_back(fmt::format("SET ivfflat.probes = {}",
                               query_opt_map.at("ivfflat.probes")));
  }

  return sqls;
}

} // namespace

void query(const DataSet *dataset, const ClientFactory *cf,
           const std::unordered_map<std::string, std::string> &query_opt_map) {
  assert(dataset != nullptr);
  assert(cf != nullptr);

  // Find k2 nearest neighbors for each query vector, recall rate is k1@k2
  auto top_k1_opt = Util::getValueFromMap(query_opt_map, "k1");
  auto top_k2_opt = Util::getValueFromMap(query_opt_map, "k2");

  size_t top_k2 = dataset->gt_topk_;
  if (top_k2_opt.has_value()) {
    top_k2 = std::stol(top_k2_opt.value());
    if (top_k2 > dataset->gt_topk_ || top_k2 <= 0) {
      SPDLOG_ERROR("Illegal k2 value: {}", top_k2);
      std::exit(1);
    }
  }

  size_t top_k1 = top_k2;
  if (top_k1_opt.has_value()) {
    top_k1 = std::stol(top_k1_opt.value());
    if (top_k1 > top_k2 || top_k1 <= 0 || top_k1 > dataset->gt_topk_) {
      SPDLOG_ERROR("Illegal k1 value: {}", top_k1);
    }
  }

  std::vector<std::string> queries;
  std::vector<std::vector<int64_t>> gts;
  auto table_name = Util::getValueFromMap(query_opt_map, "table_name");

  if (dataset->format_ == DataSetFormat::FVECS_FORMAT) {
    queries = prepareVecsQueries<float>(dataset, table_name, top_k2);
    gts = prepareVecsGroudTruths(dataset, top_k1);
  } else if (dataset->format_ == DataSetFormat::BVECS_FORMAT) {
    queries = prepareVecsQueries<uint8_t>(dataset, table_name, top_k2);
    gts = prepareVecsGroudTruths(dataset, top_k1);
  } else {
    assert(dataset->format_ == DataSetFormat::PARQUET_FORMAT);
    if (dataset->base_type_ == DataSetBaseType::FLOAT) {
      queries = prepareParquetQueries<float>(dataset, table_name, top_k2);
    } else {
      assert(dataset->base_type_ == DataSetBaseType::DOUBLE);
      queries = prepareParquetQueries<double>(dataset, table_name, top_k2);
    }
    gts = prepareParquetGroundTruths(dataset, top_k1);
  }

  size_t thread_num = std::thread::hardware_concurrency() * 2;
  // parse thread num
  auto tn = Util::getValueFromMap(query_opt_map, "thread_num");
  if (tn.has_value()) {
    thread_num = std::stoul(tn.value());
  }

  // parse loop
  size_t loop = 1;
  auto lp = Util::getValueFromMap(query_opt_map, "loop");
  if (lp.has_value()) {
    loop = std::stoul(lp.value());
  }

  // parse percentages
  std::vector<std::pair<std::string, double>> percentages;
  auto pct = Util::getValueFromMap(query_opt_map, "percentages");
  if (pct.has_value()) {
    CSVParser::parseLine(*pct, [&](std::string &token) {
      double val = std::stod(token);
      percentages.emplace_back(token, val);
    });
  }

  // generate query options sql
  std::vector<std::string> queryOptions = generateQueryOptions(query_opt_map);

  // count of generated queries
  const size_t count = queries.size();
  // execute loop times for all queries
  const size_t vcount = count * loop;

  Percentile<uint32_t> p_latencies(true);
  Percentile<float> p_recalls(false);
  std::vector<uint32_t> latencies(vcount, 0);
  std::vector<float> recalls(count, 0.0);
  // each query return top_k2 ann
  std::vector<std::vector<int64_t>> labels(count, std::vector<int64_t>(top_k2));

  std::vector<std::thread> threads;
  std::atomic<size_t> cursor{0};
  auto all_start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < thread_num; i++) {
    threads.emplace_back([&]() {
      auto client = cf->createClient();
      // set query options if necessary
      for (const auto &queryOption : queryOptions) {
        auto ret = client->executeQuery(
            queryOption.c_str(), [&](PGresult *res) -> bool {
              // no need to handle result
              assert(PQresultStatus(res) == PGRES_COMMAND_OK);
              SPDLOG_DEBUG("successfully excuted: {}", queryOption);
              return true;
            });
        if (!ret) {
          SPDLOG_ERROR("failed to execute: {}", queryOption);
        }
      }
      while (true) {
        size_t idx = cursor.fetch_add(1);
        if (idx >= vcount) {
          break;
        }
        size_t q_idx = idx % count;

        auto start = std::chrono::high_resolution_clock::now();
        auto ret = client->executeQuery(
            queries[q_idx].c_str(), [&](PGresult *res) -> bool {
              int num_rows = PQntuples(res);
              for (int j = 0; j < num_rows; j++) {
                const char *int_value_str = PQgetvalue(res, j, 0);
                labels[q_idx][j] = std::stoi(int_value_str);
              }
              return true;
            });
        auto end = std::chrono::high_resolution_clock::now();
        uint32_t microseconds =
            (std::chrono::duration_cast<std::chrono::microseconds>)(end - start)
                .count();
        latencies[idx] = microseconds;
        SPDLOG_DEBUG("query {}: {}, execution time: {}", q_idx, queries[q_idx],
                     microseconds);
        if (!ret) {
          SPDLOG_ERROR("failed to excute query {}", queries[idx]);
        }
      }
    });
  }

  for (size_t t = 0; t < thread_num; t++) {
    threads[t].join();
  }

  auto all_end = std::chrono::high_resolution_clock::now();
  double qps =
      1000000.0f * vcount /
      ((std::chrono::duration_cast<std::chrono::microseconds>)(all_end -
                                                               all_start)
           .count());

  // calculate recalls
  for (size_t i = 0; i < count; i++) {
    std::sort(labels[i].begin(), labels[i].end());
    const auto &ls = labels[i];
    const auto &gs = gts[i];
    size_t ig = 0, il = 0, correct = 0;
    while (ig < top_k1 && il < top_k2) {
      int64_t diff = gs[ig] - ls[il];
      if (diff < 0) {
        ig++;
      } else if (diff > 0) {
        il++;
      } else {
        ig++;
        il++;
        correct++;
      }
    }
    float rate = (float)correct / top_k1;
    recalls[i] = rate;
  }

  p_latencies.add(latencies.data(), vcount);
  p_recalls.add(recalls.data(), count);
  SPDLOG_INFO("qps: {}", qps);
  SPDLOG_INFO("latency(us): {}", percentile2str(p_latencies, percentages));
  SPDLOG_INFO("recall: {}", percentile2str(p_recalls, percentages));
}

} // namespace pgvectorbench
