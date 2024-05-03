#include <filesystem>
#include <stdexcept>
#include <unordered_map>

#include "dataset.h"

namespace pgvectorbench {

namespace {

const std::string default_vecs_location = "/opt/datasets/vecs/";
const std::string default_parquet_location = "/opt/datasets/parquet/";

std::unordered_map<std::string, std::shared_ptr<DataSet>> ds_map = {
    {"siftsmall",
     std::shared_ptr<DataSet>(
         new DataSet(default_vecs_location + "siftsmall/", // location
                     "siftsmall",                          // name
                     DataSetFormat::FVECS_FORMAT,          // format
                     DataSetBaseType::FLOAT,               // base type
                     DataSetMetric::L2,                    // metric
                     {std::make_pair("siftsmall_base.fvecs", 10000)},
                     {std::make_pair("id", "int"),
                      std::make_pair("embedding", "vector(128)")}, // fields
                     {},          // filter field
                     "embedding", // vector field
                     128,         // dimension
                     10000,       // nb base vectors
                     std::make_pair("siftsmall_query.fvecs", 100),
                     std::make_pair("siftsmall_groundtruth.ivecs", 100),
                     100 /* gt_topk */))},
    {"sift",
     std::shared_ptr<DataSet>(new DataSet(
         default_vecs_location + "sift/", // location
         "sift",                          // name
         DataSetFormat::FVECS_FORMAT,     // format
         DataSetBaseType::FLOAT,          // base type
         DataSetMetric::L2,               // metric
         {std::make_pair("sift_base.fvecs", 1000000)},
         {std::make_pair("id", "int"),
          std::make_pair("embedding", "vector(128)")}, // fields
         {},                                           // filter field
         "embedding",                                  // vector field
         128,                                          // dimension
         1000000,                                      // nb base vectors
         std::make_pair("sift_query.fvecs", 10000),
         std::make_pair("sift_groundtruth.ivecs", 10000), 100 /* gt_topk */
         ))},
    {"gist",
     std::shared_ptr<DataSet>(new DataSet(
         default_vecs_location + "gist/", // location
         "gist",                          // name
         DataSetFormat::FVECS_FORMAT,     // format
         DataSetBaseType::FLOAT,          // base type
         DataSetMetric::L2,               // metric
         {std::make_pair("gist_base.fvecs", 1000000)},
         {std::make_pair("id", "int"),
          std::make_pair("embedding", "vector(960)")}, // fields
         {},                                           // filter field
         "embedding",                                  // vector field
         960,                                          // demension
         1000000,                                      // nb base vectors
         std::make_pair("gist_query.fvecs", 1000),
         std::make_pair("gist_groundtruth.ivecs", 1000), 100 /* gt_topk */
         ))},
    {"glove",
     std::shared_ptr<DataSet>(new DataSet(
         default_vecs_location + "glove-100/", // location
         "glove",                              // name
         DataSetFormat::FVECS_FORMAT,          // format
         DataSetBaseType::FLOAT,               // base type
         DataSetMetric::L2,                    // metric
         {std::make_pair("glove-100_base.fvecs", 1183514)},
         {std::make_pair("id", "int"),
          std::make_pair("embedding", "vector(100)")}, // fields
         {},                                           // filter field
         "embedding",                                  // vector field
         100,                                          // dimension
         1183514,                                      // nb base vectors
         std::make_pair("glove-100_query.fvecs", 10000),
         std::make_pair("glove-100_groundtruth.ivecs", 10000), 100 /* gt_topk */
         ))},
    {"crawl",
     std::shared_ptr<DataSet>(
         new DataSet(default_vecs_location + "sift/", // location
                     "crawl",                         // name
                     DataSetFormat::FVECS_FORMAT,     // format
                     DataSetBaseType::FLOAT,          // base type
                     DataSetMetric::L2,               // metric
                     {std::make_pair("crawl_base.fvecs", 1989995)},
                     {std::make_pair("id", "int"),
                      std::make_pair("embedding", "vector(300)")}, // fields
                     {},          // filter field
                     "embedding", // vector field
                     300,         // dimension
                     1989995,     // nb base vectors
                     std::make_pair("crawl_query.fvecs", 10000),
                     std::make_pair("crawl_groundtruth.ivecs", 10000), 100))},
    {"deep1B",
     std::shared_ptr<DataSet>(new DataSet(
         default_vecs_location + "deep1B/", // location
         "deep1B",                          // name
         DataSetFormat::FVECS_FORMAT,       // format
         DataSetBaseType::FLOAT,            // base type
         DataSetMetric::L2,                 // metric
         {std::make_pair("deep1B_base.fvecs", 1000000000)},
         {std::make_pair("id", "int"),
          std::make_pair("embedding", "vector(96)")}, // fields
         {},                                          // filter field
         "embedding",                                 // vector field
         96,                                          // dimension
         1000000000,                                  // nb base vectors
         std::make_pair("deep1B_queries.fvecs", 10000),
         std::make_pair("deep1B_groundtruth.ivecs", 10000), 1 /* gt_topk */
         ))},
    {"cohere_small_100k",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "cohere_small_100k", // location
         "cohere_small_100k",                            // name
         DataSetFormat::PARQUET_FORMAT,                  // format
         DataSetBaseType::FLOAT,                         // base type
         DataSetMetric::COSINE,                          // metric
         {std::make_pair("train.parquet", 100000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(768)")}, // fields
         {},                                     // filter field
         "emb",                                  // vector field
         768,                                    // dimension
         100000,                                 // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"cohere_small_100k_filter1",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "cohere_small_100k", // location
         "cohere_small_100k_filter1",                    // name
         DataSetFormat::PARQUET_FORMAT,                  // format
         DataSetBaseType::FLOAT,                         // base type
         DataSetMetric::COSINE,                          // metric
         {std::make_pair("shuffle_train.parquet", 100000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(768)")},        // fields
         {std::make_tuple("", "id", ">=", "1000", "")}, // filter field
         "emb",                                         // vector field
         768,                                           // dimension
         100000,                                        // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_head_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"cohere_small_100k_filter99",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "cohere_small_100k", // location
         "cohere_small_100k_filter99",                   // name
         DataSetFormat::PARQUET_FORMAT,                  // format
         DataSetBaseType::FLOAT,                         // base type
         DataSetMetric::COSINE,                          // metric
         {std::make_pair("shuffle_train.parquet", 100000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(768)")},         // fields
         {std::make_tuple("", "id", ">=", "99000", "")}, // filter field
         "emb",                                          // vector field
         768,                                            // dimension
         100000,                                         // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_tail_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"cohere_medium_1m",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "cohere_medium_1m", // location
         "cohere_medium_1m",                            // name
         DataSetFormat::PARQUET_FORMAT,                 // format
         DataSetBaseType::FLOAT,                        // base type
         DataSetMetric::COSINE,                         // metric
         {std::make_pair("train.parquet", 1000000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(768)")}, // fields
         {},                                     // filter field
         "emb",                                  // vector field
         768,                                    // dimension
         1000000,                                // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"cohere_medium_1m_filter1",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "cohere_medium_1m", // location
         "cohere_medium_1m_filter1",                    // name
         DataSetFormat::PARQUET_FORMAT,                 // format
         DataSetBaseType::FLOAT,                        // base type
         DataSetMetric::COSINE,                         // metric
         {std::make_pair("shuffle_train.parquet", 1000000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(768)")},         // fields
         {std::make_tuple("", "id", ">=", "10000", "")}, // filter field
         "emb",                                          // vector field
         768,                                            // dimension
         1000000,                                        // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_head_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"cohere_medium_1m_filter99",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "cohere_medium_1m", // location
         "cohere_medium_1m_filter99",                   // name
         DataSetFormat::PARQUET_FORMAT,                 // format
         DataSetBaseType::FLOAT,                        // base type
         DataSetMetric::COSINE,                         // metric
         {std::make_pair("shuffle_train.parquet", 1000000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(768)")},          // fields
         {std::make_tuple("", "id", ">=", "990000", "")}, // filter field
         "emb",                                           // vector field
         768,                                             // dimension
         1000000,                                         // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_tail_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"cohere_large_10m",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "cohere_large_10m", // location
         "cohere_large_10m",                            // name
         DataSetFormat::PARQUET_FORMAT,                 // format
         DataSetBaseType::FLOAT,                        // base type
         DataSetMetric::COSINE,                         // metric
         {std::make_pair("train-00-of-10.parquet", 1000000),
          std::make_pair("train-01-of-10.parquet", 1000000),
          std::make_pair("train-02-of-10.parquet", 1000000),
          std::make_pair("train-03-of-10.parquet", 1000000),
          std::make_pair("train-04-of-10.parquet", 1000000),
          std::make_pair("train-05-of-10.parquet", 1000000),
          std::make_pair("train-06-of-10.parquet", 1000000),
          std::make_pair("train-07-of-10.parquet", 1000000),
          std::make_pair("train-08-of-10.parquet", 1000000),
          std::make_pair("train-09-of-10.parquet", 1000000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(768)")}, // fields
         {},                                     // filter field
         "emb",                                  // vector field
         768,                                    // dimension
         10000000,                               // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"cohere_large_10m_filter1",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "cohere_large_10m", // location
         "cohere_large_10m_filter1",                    // name
         DataSetFormat::PARQUET_FORMAT,                 // format
         DataSetBaseType::FLOAT,                        // base type
         DataSetMetric::COSINE,                         // metric
         {std::make_pair("shuffle_train-00-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-01-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-02-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-03-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-04-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-05-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-06-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-07-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-08-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-09-of-10.parquet", 1000000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(768)")},          // fields
         {std::make_tuple("", "id", ">=", "100000", "")}, // filter field
         "emb",                                           // vector field
         768,                                             // dimension
         10000000,                                        // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_head_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"cohere_large_10m_filter99",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "cohere_large_10m", // location
         "cohere_large_10m_filter99",                   // name
         DataSetFormat::PARQUET_FORMAT,                 // format
         DataSetBaseType::FLOAT,                        // base type
         DataSetMetric::COSINE,                         // metric
         {std::make_pair("shuffle_train-00-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-01-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-02-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-03-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-04-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-05-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-06-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-07-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-08-of-10.parquet", 1000000),
          std::make_pair("shuffle_train-09-of-10.parquet", 1000000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(768)")},           // fields
         {std::make_tuple("", "id", ">=", "9900000", "")}, // filter field
         "emb",                                            // vector field
         768,                                              // dimension
         10000000,                                         // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_tail_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"openai_small_50k",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "openai_small_50k", // location
         "openai_small_50k",                            // name
         DataSetFormat::PARQUET_FORMAT,                 // format
         DataSetBaseType::DOUBLE,                       // base type
         DataSetMetric::COSINE,                         // metric
         {std::make_pair("train.parquet", 50000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(1536)")}, // fields
         {},                                      // filter field
         "emb",                                   // vector field
         1536,                                    // dimension
         50000,                                   // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"openai_small_50k_filter1",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "openai_small_50k", // location
         "openai_small_50k_filter1",                    // name
         DataSetFormat::PARQUET_FORMAT,                 // format
         DataSetBaseType::DOUBLE,                       // base type
         DataSetMetric::COSINE,                         // metric
         {std::make_pair("shuffle_train.parquet", 50000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(1536)")},      // fields
         {std::make_tuple("", "id", ">=", "500", "")}, // filter field
         "emb",                                        // vector field
         1536,                                         // dimension
         50000,                                        // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_head_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"openai_small_50k_filter99",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "openai_small_50k", // location
         "openai_small_50k_filter99",                   // name
         DataSetFormat::PARQUET_FORMAT,                 // format
         DataSetBaseType::DOUBLE,                       // base type
         DataSetMetric::COSINE,                         // metric
         {std::make_pair("shuffle_train.parquet", 50000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(1536)")},        // fields
         {std::make_tuple("", "id", ">=", "49500", "")}, // filter field
         "emb",                                          // vector field
         1536,                                           // dimension
         50000,                                          // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_tail_1p.parquet", 1000), 500 /* gt_topk */
         ))},
    {"openai_medium_500k",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "openai_medium_500k", // location
         "openai_medium_500k",                            // name
         DataSetFormat::PARQUET_FORMAT,                   // format
         DataSetBaseType::DOUBLE,                         // base type
         DataSetMetric::COSINE,                           // metric
         {std::make_pair("train.parquet", 500000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(1536)")}, // fields
         {},                                      // filter field
         "emb",                                   // vector field
         1536,                                    // dimension
         500000,                                  // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"openai_medium_500k_filter1",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "openai_medium_500k", // location
         "openai_medium_500k_filter1",                    // name
         DataSetFormat::PARQUET_FORMAT,                   // format
         DataSetBaseType::DOUBLE,                         // base type
         DataSetMetric::COSINE,                           // metric
         {std::make_pair("shuffle_train.parquet", 500000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(1536)")},       // fields
         {std::make_tuple("", "id", ">=", "5000", "")}, // filter field
         "emb",                                         // vector field
         1536,                                          // dimension
         500000,                                        // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_head_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"openai_medium_500k_filter99",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "openai_medium_500k", // location
         "openai_medium_500k_filter99",                   // name
         DataSetFormat::PARQUET_FORMAT,                   // format
         DataSetBaseType::DOUBLE,                         // base type
         DataSetMetric::COSINE,                           // metric
         {std::make_pair("shuffle_train.parquet", 500000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(1536)")},         // fields
         {std::make_tuple("", "id", ">=", "495000", "")}, // filter field
         "emb",                                           // vector field
         1536,                                            // dimension
         500000,                                          // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_tail_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"openai_large_5m",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "openai_large_5m", // location
         "openai_large_5m",                            // name
         DataSetFormat::PARQUET_FORMAT,                // format
         DataSetBaseType::DOUBLE,                      // base type
         DataSetMetric::COSINE,                        // metric
         {std::make_pair("train-00-of-10.parquet", 500000),
          std::make_pair("train-01-of-10.parquet", 500000),
          std::make_pair("train-02-of-10.parquet", 500000),
          std::make_pair("train-03-of-10.parquet", 500000),
          std::make_pair("train-04-of-10.parquet", 500000),
          std::make_pair("train-05-of-10.parquet", 500000),
          std::make_pair("train-06-of-10.parquet", 500000),
          std::make_pair("train-07-of-10.parquet", 500000),
          std::make_pair("train-08-of-10.parquet", 500000),
          std::make_pair("train-09-of-10.parquet", 500000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(1536)")}, // fields
         {},                                      // filter field
         "emb",                                   // vector field
         1536,                                    // dimension
         5000000,                                 // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"openai_large_5m_filter1",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "openai_large_5m", // location
         "openai_large_5m_filter1",                    // name
         DataSetFormat::PARQUET_FORMAT,                // format
         DataSetBaseType::DOUBLE,                      // base type
         DataSetMetric::COSINE,                        // metric
         {std::make_pair("shuffle_train-00-of-10.parquet", 500000),
          std::make_pair("shuffle_train-01-of-10.parquet", 500000),
          std::make_pair("shuffle_train-02-of-10.parquet", 500000),
          std::make_pair("shuffle_train-03-of-10.parquet", 500000),
          std::make_pair("shuffle_train-04-of-10.parquet", 500000),
          std::make_pair("shuffle_train-05-of-10.parquet", 500000),
          std::make_pair("shuffle_train-06-of-10.parquet", 500000),
          std::make_pair("shuffle_train-07-of-10.parquet", 500000),
          std::make_pair("shuffle_train-08-of-10.parquet", 500000),
          std::make_pair("shuffle_train-09-of-10.parquet", 500000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(1536)")},        // fields
         {std::make_tuple("", "id", ">=", "50000", "")}, // filter field
         "emb",                                          // vector field
         1536,                                           // dimension
         5000000,                                        // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_head_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"openai_large_5m_filter99",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "openai_large_5m", // location
         "openai_large_5m_filter99",                   // name
         DataSetFormat::PARQUET_FORMAT,                // format
         DataSetBaseType::DOUBLE,                      // base type
         DataSetMetric::COSINE,                        // metric
         {std::make_pair("shuffle_train-00-of-10.parquet", 500000),
          std::make_pair("shuffle_train-01-of-10.parquet", 500000),
          std::make_pair("shuffle_train-02-of-10.parquet", 500000),
          std::make_pair("shuffle_train-03-of-10.parquet", 500000),
          std::make_pair("shuffle_train-04-of-10.parquet", 500000),
          std::make_pair("shuffle_train-05-of-10.parquet", 500000),
          std::make_pair("shuffle_train-06-of-10.parquet", 500000),
          std::make_pair("shuffle_train-07-of-10.parquet", 500000),
          std::make_pair("shuffle_train-08-of-10.parquet", 500000),
          std::make_pair("shuffle_train-09-of-10.parquet", 500000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(1536)")},          // fields
         {std::make_tuple("", "id", ">=", "4950000", "")}, // filter field
         "emb",                                            // vector field
         1536,                                             // dimension
         5000000,                                          // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors_tail_1p.parquet", 1000), 1000 /* gt_topk */
         ))},
    {"laion_large_100m",
     std::shared_ptr<DataSet>(new DataSet(
         default_parquet_location + "laion_large_100m", // location
         "laion_large_100m",                            // name
         DataSetFormat::PARQUET_FORMAT,                 // format
         DataSetBaseType::FLOAT,                        // base type
         DataSetMetric::L2,                             // metric
         {std::make_pair("train-00-of-100.parquet", 1000000),
          std::make_pair("train-01-of-100.parquet", 1000000),
          std::make_pair("train-02-of-100.parquet", 1000000),
          std::make_pair("train-03-of-100.parquet", 1000000),
          std::make_pair("train-04-of-100.parquet", 1000000),
          std::make_pair("train-05-of-100.parquet", 1000000),
          std::make_pair("train-06-of-100.parquet", 1000000),
          std::make_pair("train-07-of-100.parquet", 1000000),
          std::make_pair("train-08-of-100.parquet", 1000000),
          std::make_pair("train-09-of-100.parquet", 1000000),
          std::make_pair("train-10-of-100.parquet", 1000000),
          std::make_pair("train-11-of-100.parquet", 1000000),
          std::make_pair("train-12-of-100.parquet", 1000000),
          std::make_pair("train-13-of-100.parquet", 1000000),
          std::make_pair("train-14-of-100.parquet", 1000000),
          std::make_pair("train-15-of-100.parquet", 1000000),
          std::make_pair("train-16-of-100.parquet", 1000000),
          std::make_pair("train-17-of-100.parquet", 1000000),
          std::make_pair("train-18-of-100.parquet", 1000000),
          std::make_pair("train-19-of-100.parquet", 1000000),
          std::make_pair("train-20-of-100.parquet", 1000000),
          std::make_pair("train-21-of-100.parquet", 1000000),
          std::make_pair("train-22-of-100.parquet", 1000000),
          std::make_pair("train-23-of-100.parquet", 1000000),
          std::make_pair("train-24-of-100.parquet", 1000000),
          std::make_pair("train-25-of-100.parquet", 1000000),
          std::make_pair("train-26-of-100.parquet", 1000000),
          std::make_pair("train-27-of-100.parquet", 1000000),
          std::make_pair("train-28-of-100.parquet", 1000000),
          std::make_pair("train-29-of-100.parquet", 1000000),
          std::make_pair("train-30-of-100.parquet", 1000000),
          std::make_pair("train-31-of-100.parquet", 1000000),
          std::make_pair("train-32-of-100.parquet", 1000000),
          std::make_pair("train-33-of-100.parquet", 1000000),
          std::make_pair("train-34-of-100.parquet", 1000000),
          std::make_pair("train-35-of-100.parquet", 1000000),
          std::make_pair("train-36-of-100.parquet", 1000000),
          std::make_pair("train-37-of-100.parquet", 1000000),
          std::make_pair("train-38-of-100.parquet", 1000000),
          std::make_pair("train-39-of-100.parquet", 1000000),
          std::make_pair("train-40-of-100.parquet", 1000000),
          std::make_pair("train-41-of-100.parquet", 1000000),
          std::make_pair("train-42-of-100.parquet", 1000000),
          std::make_pair("train-43-of-100.parquet", 1000000),
          std::make_pair("train-44-of-100.parquet", 1000000),
          std::make_pair("train-45-of-100.parquet", 1000000),
          std::make_pair("train-46-of-100.parquet", 1000000),
          std::make_pair("train-47-of-100.parquet", 1000000),
          std::make_pair("train-48-of-100.parquet", 1000000),
          std::make_pair("train-49-of-100.parquet", 1000000),
          std::make_pair("train-50-of-100.parquet", 1000000),
          std::make_pair("train-51-of-100.parquet", 1000000),
          std::make_pair("train-52-of-100.parquet", 1000000),
          std::make_pair("train-53-of-100.parquet", 1000000),
          std::make_pair("train-54-of-100.parquet", 1000000),
          std::make_pair("train-55-of-100.parquet", 1000000),
          std::make_pair("train-56-of-100.parquet", 1000000),
          std::make_pair("train-57-of-100.parquet", 1000000),
          std::make_pair("train-58-of-100.parquet", 1000000),
          std::make_pair("train-59-of-100.parquet", 1000000),
          std::make_pair("train-60-of-100.parquet", 1000000),
          std::make_pair("train-61-of-100.parquet", 1000000),
          std::make_pair("train-62-of-100.parquet", 1000000),
          std::make_pair("train-63-of-100.parquet", 1000000),
          std::make_pair("train-64-of-100.parquet", 1000000),
          std::make_pair("train-65-of-100.parquet", 1000000),
          std::make_pair("train-66-of-100.parquet", 1000000),
          std::make_pair("train-67-of-100.parquet", 1000000),
          std::make_pair("train-68-of-100.parquet", 1000000),
          std::make_pair("train-69-of-100.parquet", 1000000),
          std::make_pair("train-70-of-100.parquet", 1000000),
          std::make_pair("train-71-of-100.parquet", 1000000),
          std::make_pair("train-72-of-100.parquet", 1000000),
          std::make_pair("train-73-of-100.parquet", 1000000),
          std::make_pair("train-74-of-100.parquet", 1000000),
          std::make_pair("train-75-of-100.parquet", 1000000),
          std::make_pair("train-76-of-100.parquet", 1000000),
          std::make_pair("train-77-of-100.parquet", 1000000),
          std::make_pair("train-78-of-100.parquet", 1000000),
          std::make_pair("train-79-of-100.parquet", 1000000),
          std::make_pair("train-80-of-100.parquet", 1000000),
          std::make_pair("train-81-of-100.parquet", 1000000),
          std::make_pair("train-82-of-100.parquet", 1000000),
          std::make_pair("train-83-of-100.parquet", 1000000),
          std::make_pair("train-84-of-100.parquet", 1000000),
          std::make_pair("train-85-of-100.parquet", 1000000),
          std::make_pair("train-86-of-100.parquet", 1000000),
          std::make_pair("train-87-of-100.parquet", 1000000),
          std::make_pair("train-88-of-100.parquet", 1000000),
          std::make_pair("train-89-of-100.parquet", 1000000),
          std::make_pair("train-90-of-100.parquet", 1000000),
          std::make_pair("train-91-of-100.parquet", 1000000),
          std::make_pair("train-92-of-100.parquet", 1000000),
          std::make_pair("train-93-of-100.parquet", 1000000),
          std::make_pair("train-94-of-100.parquet", 1000000),
          std::make_pair("train-95-of-100.parquet", 1000000),
          std::make_pair("train-96-of-100.parquet", 1000000),
          std::make_pair("train-97-of-100.parquet", 1000000),
          std::make_pair("train-98-of-100.parquet", 1000000),
          std::make_pair("train-99-of-100.parquet", 1000000)},
         {std::make_pair("id", "int8"),
          std::make_pair("emb", "vector(1536)")}, // fields
         {},                                      // filter field
         "emb",                                   // vector field
         768,                                     // dimension
         100000000,                               // nb base vectors
         std::make_pair("test.parquet", 1000),
         std::make_pair("neighbors.parquet", 1000), 1000 /* gt_topk */
         ))},
}; // namespace

std::string_view datasetFormatString(enum DataSetFormat dsf) {
  switch (dsf) {
  case DataSetFormat::FVECS_FORMAT:
    return std::string_view{"FVECS"};
  case DataSetFormat::BVECS_FORMAT:
    return std::string_view{"BVECS"};
  case DataSetFormat::PARQUET_FORMAT:
    return std::string_view{"PARQUET"};
  default:
    throw std::runtime_error("Unsupported format!!!");
  }
}

} // namespace

std::ostream &operator<<(std::ostream &os, const DataSet &dataset) {
  os << "dataset name: " << dataset.name_ << "\n";
  os << "location: " << dataset.location_ << "\n";
  os << "format: " << datasetFormatString(dataset.format_) << "\n";
  os << "metric: " << metric2ops(dataset.metric_) << "\n";
  os << "vector dimension: " << dataset.dim_ << "\n";
  os << "base files:\n";
  for (const auto &base_file : dataset.base_files_) {
    os << "  filename: " << base_file.first
       << ", row count: " << base_file.second << "\n";
  }
  os << "total row cnt: " << dataset.total_cnt_ << "\n";
  os << "fields:\n";
  for (const auto &field : dataset.fields_) {
    os << "  field name: " << field.first << ", field type: " << field.second
       << "\n";
  }
  os << "vector field: " << dataset.vector_field_ << "\n";
  os << "query file: " << dataset.query_file_.first
     << ", query cnt: " << dataset.query_file_.second << "\n";
  os << "ground truth file: " << dataset.gt_file_.first
     << ", gt cnt: " << dataset.gt_file_.second
     << ", gt_topk: " << dataset.gt_topk_;
  return os;
}

DataSet *getDataSet(const std::string &ds_name) {
  if (ds_map.find(ds_name) != ds_map.end()) {
    return ds_map.at(ds_name).get();
  }

  return nullptr;
}

std::string_view metric2ops(enum DataSetMetric metric) {
  switch (metric) {
  case DataSetMetric::L1:
    return std::string_view{"vector_l1_ops"};
  case DataSetMetric::L2:
    return std::string_view{"vector_l2_ops"};
  case DataSetMetric::IP:
    return std::string_view{"vector_ip_ops"};
  case DataSetMetric::COSINE:
    return std::string_view{"vector_cosine_ops"};
  case DataSetMetric::HAMMING:
    return std::string_view{"bit_hamming_ops"};
  case DataSetMetric::JACCARD:
    return std::string_view{"bit_jaccard_ops"};
  default:
    throw std::runtime_error("Unsupported metric!!!");
  }
}

std::string_view metric2operator(enum DataSetMetric metric) {
  switch (metric) {
  case DataSetMetric::L1:
    return std::string_view{"<+>"};
  case DataSetMetric::L2:
    return std::string_view{"<->"};
  case DataSetMetric::IP:
    return std::string_view{"<#>"};
  case DataSetMetric::COSINE:
    return std::string_view{"<=>"};
  case DataSetMetric::HAMMING:
    return std::string_view{"<~>"};
  case DataSetMetric::JACCARD:
    return std::string_view{"<%>"};
  default:
    throw std::runtime_error("Unsupported metric!!!");
  }
}

} // namespace pgvectorbench
