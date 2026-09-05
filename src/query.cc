#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>

// third party
#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <ryu/ryu.h>

#include "dataset/dataset.h"
#include "dataset/parquet_embedding.h"
#include "dataset/parquet_ground_truth.h"
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
  if (filesize % rowsize != 0 || filesize / rowsize != rowcnt) {
    throw std::runtime_error("VECS query file size does not match dataset");
  }

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
    uint32_t dim;
    std::memcpy(&dim, buffer, sizeof(dim));
    if (dim != dataset->dim_) {
      throw std::runtime_error("VECS query dimension does not match dataset");
    }

    oss << "'[";
    if constexpr (std::is_same_v<DataType, float>) {
      for (size_t j = 0; j < dim; j++) {
        float value;
        std::memcpy(&value, buffer + sizeof(dim) + j * sizeof(value),
                    sizeof(value));
        f2s_buffered(value, result);
        oss << result;
        if (j != dim - 1) {
          oss << ',';
        }
      }
    } else {
      for (size_t j = 0; j < dim; j++) {
        oss << static_cast<unsigned int>(
            static_cast<uint8_t>(buffer[sizeof(dim) + j]));
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
  if (top_k1 == 0 || top_k1 > dataset->gt_topk_) {
    throw std::runtime_error("invalid ground-truth top_k");
  }
  // ground truth file path
  auto file_path = dataset->location_ + dataset->gt_file_.first;
  std::unique_ptr<util::FileReader> reader =
      std::make_unique<util::FileReader>(file_path);
  reader->open();
  const size_t filesize = reader->filesize();
  const size_t rowsize = sizeof(uint32_t) + sizeof(int32_t) * dataset->gt_topk_;
  const size_t rowcnt = dataset->gt_file_.second;
  if (filesize % rowsize != 0 || filesize / rowsize != rowcnt) {
    throw std::runtime_error(
        "VECS ground-truth file size does not match dataset");
  }
  std::vector<std::vector<int64_t>> gts(rowcnt, std::vector<int64_t>(top_k1));

  std::string buffer_(rowsize, ' ');
  char *buffer = buffer_.data();

  for (size_t i = 0; i < rowcnt; i++) {
    reader->read(buffer, rowsize, rowsize * i);
    uint32_t dim;
    std::memcpy(&dim, buffer, sizeof(dim));
    if (dim != dataset->gt_topk_) {
      throw std::runtime_error(
          "VECS ground-truth dimension does not match top_k");
    }
    for (size_t j = 0; j < top_k1; j++) {
      int32_t neighbor;
      std::memcpy(&neighbor, buffer + sizeof(dim) + j * sizeof(neighbor),
                  sizeof(neighbor));
      gts[i][j] = neighbor;
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

  auto rb_reader_result = arrow_reader->GetRecordBatchReader();
  if (!rb_reader_result.ok()) {
    SPDLOG_ERROR("get record batch reader failed: {}",
                 rb_reader_result.status().ToString());
    std::exit(1);
  }
  auto rb_reader = std::move(rb_reader_result).ValueOrDie();

  std::vector<std::string> queries(dataset->query_file_.second);
  size_t rows = 0;

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
      const auto &ids = parquetRowIds(*recordBatch);
      for (int64_t i = 0; i < recordBatch->num_rows(); ++i) {
        if (ids.IsNull(i) || ids.Value(i) < 0 ||
            static_cast<size_t>(ids.Value(i)) >= queries.size()) {
          throw std::runtime_error("query row id is null or out of range");
        }
        auto &sql = queries[ids.Value(i)];
        if (!sql.empty()) {
          throw std::runtime_error("duplicate query row id");
        }
        sql = sql_prefix + "'" +
              formatParquetEmbedding<DataType>(*recordBatch->column(1), i,
                                               dataset->dim_) +
              "' LIMIT " + std::to_string(top_k2) + ";";
        ++rows;
      }
    }
  } while (recordBatch);

  if (rows != queries.size()) {
    throw std::runtime_error("Parquet query row count does not match dataset");
  }
  return queries;
}

std::vector<std::vector<int64_t>>
prepareParquetGroundTruths(const DataSet *dataset, size_t top_k1) {
  if (top_k1 == 0 || top_k1 > dataset->gt_topk_) {
    throw std::runtime_error("invalid ground-truth top_k");
  }
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

  auto rb_reader_result = arrow_reader->GetRecordBatchReader();
  if (!rb_reader_result.ok()) {
    SPDLOG_ERROR("get record batch reader failed: {}",
                 rb_reader_result.status().ToString());
    std::exit(1);
  }
  auto rb_reader = std::move(rb_reader_result).ValueOrDie();

  std::vector<std::vector<int64_t>> gts(dataset->gt_file_.second,
                                        std::vector<int64_t>(top_k1));
  std::vector<bool> seen(gts.size(), false);
  size_t rows = 0;

  std::shared_ptr<arrow::RecordBatch> recordBatch;
  do {
    status = rb_reader->ReadNext(&recordBatch);
    if (!status.ok()) {
      SPDLOG_ERROR("read next batch failed: {}", status.ToString());
      std::exit(1);
    }
    if (recordBatch) {
      const auto &ids = parquetRowIds(*recordBatch);
      auto neighbors = recordBatch->column(1);
      auto copy_rows = [&](const auto &list_array) {
        auto int_array =
            std::dynamic_pointer_cast<arrow::Int64Array>(list_array.values());
        if (!int_array) {
          throw std::runtime_error("ground-truth neighbors must be int64");
        }
        for (int64_t i = 0; i < recordBatch->num_rows(); i++) {
          copyParquetGroundTruthRow(ids, list_array, *int_array, i, top_k1,
                                    gts);
          const auto id = ids.Value(i); // range checked by the row copier
          if (seen[id]) {
            throw std::runtime_error("duplicate ground-truth row id");
          }
          seen[id] = true;
          ++rows;
        }
      };

      if (neighbors->type_id() == arrow::Type::LIST) {
        copy_rows(*std::static_pointer_cast<arrow::ListArray>(neighbors));
      } else if (neighbors->type_id() == arrow::Type::LARGE_LIST) {
        copy_rows(*std::static_pointer_cast<arrow::LargeListArray>(neighbors));
      } else {
        throw std::runtime_error(
            "ground-truth neighbors must be a list or large_list");
      }
    }
  } while (recordBatch);

  if (rows != gts.size()) {
    throw std::runtime_error(
        "Parquet ground-truth row count does not match dataset");
  }
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

void query(
    const DataSet *dataset, const ClientFactory *cf,
    const std::unordered_map<std::string, std::string> &query_opt_map) try {
  assert(dataset != nullptr);
  assert(cf != nullptr);

  auto positive_option = [&](const char *name, size_t fallback,
                             size_t maximum) {
    auto value = Util::getValueFromMap(query_opt_map, name);
    if (!value) {
      return fallback;
    }
    size_t parsed = 0;
    const auto [end, error] =
        std::from_chars(value->data(), value->data() + value->size(), parsed);
    if (error != std::errc{} || end != value->data() + value->size() ||
        parsed == 0 || parsed > maximum) {
      throw std::runtime_error(fmt::format(
          "Invalid query option {}={}: expected an integer from 1 to {}", name,
          *value, maximum));
    }
    return parsed;
  };

  if (dataset->query_file_.second == 0 ||
      dataset->query_file_.second != dataset->gt_file_.second ||
      dataset->gt_topk_ == 0 || dataset->dim_ == 0) {
    throw std::runtime_error(
        "query and ground-truth counts must match and be nonzero");
  }
  // Find k2 nearest neighbors for each query vector, recall rate is k1@k2.
  const size_t top_k2 =
      positive_option("k2", dataset->gt_topk_, dataset->gt_topk_);
  const size_t top_k1 = positive_option("k1", top_k2, top_k2);
  const size_t thread_num = positive_option(
      "thread_num",
      size_t(std::max(1u, std::thread::hardware_concurrency())) * 2,
      std::numeric_limits<int>::max());
  const size_t count = dataset->query_file_.second;
  const size_t loop =
      positive_option("loop", 1, std::vector<uint32_t>().max_size() / count);
  const size_t vcount = count * loop;

  std::vector<std::pair<std::string, double>> percentages;
  if (auto pct = Util::getValueFromMap(query_opt_map, "percentages")) {
    // Do not silently discard empty fields or partially parsed numbers.
    std::istringstream input(*pct);
    std::string token;
    if (pct->empty() || pct->back() == ',') {
      throw std::runtime_error("Invalid query option percentages");
    }
    while (std::getline(input, token, ',')) {
      size_t end = 0;
      double value;
      try {
        value = std::stod(token, &end);
      } catch (const std::exception &) {
        throw std::runtime_error("Invalid query option percentages");
      }
      if (end != token.size() || !std::isfinite(value) || value < 0 ||
          value > 100) {
        throw std::runtime_error("Invalid query option percentages");
      }
      percentages.emplace_back(token, value);
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

  if (queries.size() != count || gts.size() != count) {
    throw std::runtime_error(
        "query and ground-truth counts do not match dataset");
  }
  std::vector<std::string> queryOptions = generateQueryOptions(query_opt_map);

  Percentile<uint32_t> p_latencies(true);
  Percentile<float> p_recalls(false);
  std::vector<uint32_t> latencies(vcount, 0);
  std::vector<float> recalls(count, 0.0);
  // Store only the neighbors actually returned by each query.
  std::vector<std::vector<int64_t>> labels(count);

  // Validate every connection before starting query workers.
  std::vector<std::unique_ptr<Client>> clients;
  for (size_t i = 0; i < thread_num; i++) {
    auto client = cf->createClient();
    if (!client) {
      std::exit(1);
    }
    clients.push_back(std::move(client));
  }

  std::vector<std::thread> threads;
  std::atomic<size_t> cursor{0};
  std::atomic<bool> failed{false};
  auto all_start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < thread_num; i++) {
    threads.emplace_back([&, client = std::move(clients[i])]() {
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
          failed = true;
          return;
        }
      }
      while (!failed) {
        size_t idx = cursor.fetch_add(1);
        if (idx >= vcount) {
          break;
        }
        size_t q_idx = idx % count;

        auto start = std::chrono::steady_clock::now();
        auto ret = client->executeQuery(
            queries[q_idx].c_str(), [&](PGresult *res) -> bool {
              // Only the final iteration owns this query's recall results.
              if (idx < vcount - count) {
                return true;
              }
              if (PQnfields(res) != 1) {
                SPDLOG_ERROR("query must return one id column");
                return false;
              }
              int num_rows = PQntuples(res);
              labels[q_idx].resize(num_rows);
              for (int j = 0; j < num_rows; j++) {
                const char *value = PQgetvalue(res, j, 0);
                const char *last = value + PQgetlength(res, j, 0);
                const auto [end, error] =
                    std::from_chars(value, last, labels[q_idx][j]);
                if (PQgetisnull(res, j, 0) || error != std::errc{} ||
                    end != last) {
                  SPDLOG_ERROR("query result id must be a non-null int64");
                  return false;
                }
              }
              return true;
            });
        auto end = std::chrono::steady_clock::now();
        uint32_t microseconds =
            (std::chrono::duration_cast<std::chrono::microseconds>)(end - start)
                .count();
        latencies[idx] = microseconds;
        SPDLOG_DEBUG("query {}: {}, execution time: {}", q_idx, queries[q_idx],
                     microseconds);
        if (!ret) {
          SPDLOG_ERROR("failed to excute query {}", queries[q_idx]);
          failed = true;
          break;
        }
      }
    });
  }

  for (size_t t = 0; t < thread_num; t++) {
    threads[t].join();
  }
  if (failed) {
    SPDLOG_ERROR("query benchmark failed; no statistics will be reported");
    std::exit(1);
  }

  auto all_end = std::chrono::steady_clock::now();
  const double elapsed =
      std::chrono::duration<double>(all_end - all_start).count();
  const double qps = vcount / std::max(elapsed, 1e-9);

  // calculate recalls
  for (size_t i = 0; i < count; i++) {
    std::sort(labels[i].begin(), labels[i].end());
    const auto &ls = labels[i];
    const auto &gs = gts[i];
    size_t ig = 0, il = 0, correct = 0;
    while (ig < top_k1 && il < ls.size()) {
      if (gs[ig] < ls[il]) {
        ig++;
      } else if (gs[ig] > ls[il]) {
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
} catch (const std::exception &error) {
  SPDLOG_ERROR("query benchmark failed: {}", error.what());
  std::exit(1);
}

} // namespace pgvectorbench
