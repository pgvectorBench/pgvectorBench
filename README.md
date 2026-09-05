# pgvectorbench

A lightweight, fast, flexible and easy-to-use benchmarking tool specifically designed for the performance evaluation and optimization of [pgvector](https://github.com/pgvector/pgvector).

`pgvectorbench` consists of five phases, each of which can be run **independently** or **chained** together to achieve a comprehensive benchmarking process:

- Setup: This phase involves setting up the benchmarking table and potentially creating indexes before loading data into the table. Additionally, any necessary extensions can be created during this phase.
- Load: In this phase, the dataset is loaded into the benchmarking table. Efficient data loading mechanisms are implemented to ensure that the dataset is ingested quickly and reliably, ready for subsequent phases.
- Index: After the data has been loaded, this phase is dedicated to the construction of indexes. It is designed to potentially yield more optimized index build times.
- Query: Benchmarking queries are executed in this phase, and metrics such as queries per second (QPS), latency, and recall are calculated. Latency and recall are determined using user-specified percentages.
- Teardown: This final phase involves performing any necessary cleanup tasks after the benchmarking is complete. This may include dropping indexes, truncating or dropping tables, and removing any extensions that were created during setup.

## Supported datasets

Real world dataset matters, pgvectorbench support two kinds of datasets for now:

- VECS
  - The vectors are stored in raw little endian. Each vector takes 4+d*4 bytes for .fvecs and .ivecs formats, and 4+d bytes for .bvecs formats, 
where d is the dimensionality of the vector.
- Parquet
  - Curated by Zilliz, is uniformly structured in the efficient Parquet file format. Use `aws s3 ls s3://assets.zilliz.com/benchmark/ --region us-west-2 --no-sign-request` to list all datasets.
  - In specific use cases, complex query formulations can be designed to include supplementary filter conditions on non-vector attributes, thereby refining the search criteria.

Datasets details:

| Dataset | Format | Metric | dim | nb base vectors | nb query vectors | Download |
|---------|--------|--------|----:|----------------:|-------------:|----------|
| [siftsmall](http://corpus-texmex.irisa.fr/) | VECS | L2 | 128 | 10,000 | 100 | wget ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz |
| [sift](http://corpus-texmex.irisa.fr/) | VECS | L2 | 128 | 1,000,000 | 10,000 | wget ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz |
| [gist](http://corpus-texmex.irisa.fr/) | VECS | L2 | 960 | 1,000,000 | 1,000 | wget ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz |
| [glove](https://nlp.stanford.edu/projects/glove/) | VECS | L2 | 100 | 1,183,514 | 10,000 | wget http://downloads.zjulearning.org.cn/data/glove-100.tar.gz |
| [crawl](https://commoncrawl.org/) | VECS |  L2 | 300 | 1,989,995 | 10,000 | wget http://downloads.zjulearning.org.cn/data/crawl.tar.gz |
| [deep1B](http://sites.skoltech.ru/compvision/noimi/) | VECS | L2 | 96 | 1,000,000,000 | 10,000 | https://yadi.sk/d/11eDCm7Dsn9GA chunks must be concatenated into one file(deep1B_base.fvecs) before loading |
| cohere_small_100k | Parquet | COSINE | 768 | 100,000 | 1,000 | aws s3 cp s3://assets.zilliz.com/benchmark/cohere_small_100k/ ./cohere_small_100k/ --region us-west-2 --recursive --no-sign-request |
| cohere_small_100k_filter1 | Parquet | COSINE | 768 | 100,000 | 1,000 | same  as 👆🏻 |
| cohere_small_100k_filter99 | Parquet | COSINE | 768 | 100,000 | 1,000 | same  as 👆🏻 |
| cohere_medium_1m | Parquet | COSINE | 768 | 1,000,000 | 1,000 | aws s3 cp s3://assets.zilliz.com/benchmark/cohere_medium_1m/ ./cohere_medium_1m/ --region us-west-2 --recursive --no-sign-request |
| cohere_medium_1m_filter1 | Parquet | COSINE | 768 | 1,000,000 | 1,000 | same  as 👆🏻 |
| cohere_medium_1m_filter99 | Parquet | COSINE | 768 | 1,000,000 | 1,000 | same  as 👆🏻 |
| cohere_large_10m | Parquet | COSINE | 768 | 10,000,000 | 1,000 | aws s3 cp s3://assets.zilliz.com/benchmark/cohere_large_10m/ ./cohere_large_10m/ --region us-west-2 --recursive |
| cohere_large_10m_filter1 | Parquet | COSINE | 768 | 10,000,000 | 1,000 | same  as 👆🏻 |
| cohere_large_10m_filter99 | Parquet | COSINE | 768 | 10,000,000 | 1,000 | same  as 👆🏻 |
| openai_small_50k | Parquet | COSINE | 1536 | 50,000 | 1,000 | aws s3 cp s3://assets.zilliz.com/benchmark/openai_small_50k/ ./openai_small_50k/ --region us-west-2 --recursive --no-sign-request |
| openai_small_50k_filter1 | Parquet | COSINE | 1536 | 50,000 | 1,000 | same  as 👆🏻 |
| openai_small_50k_filter99 | Parquet | COSINE | 1536 | 50,000 | 1,000 | same  as 👆🏻 |
| openai_medium_500k | Parquet | COSINE | 1536 | 500,000 | 1,000 | aws s3 cp s3://assets.zilliz.com/benchmark/openai_medium_500k/ ./openai_medium_500k/ --region us-west-2 --recursive --no-sign-request |
| openai_medium_500k_filter1 | Parquet | COSINE | 1536 | 500,000 | 1,000 | same  as 👆🏻 |
| openai_medium_500k_filter99 | Parquet | COSINE | 1536 | 500,000 | 1,000 | same  as 👆🏻 |
| openai_large_5m | Parquet | COSINE | 1536 | 5,000,000 | 1,000 | aws s3 cp s3://assets.zilliz.com/benchmark/openai_large_5m/ ./openai_large_5m/ --region us-west-2 --recursive --no-sign-request |
| openai_large_5m_filter1 | Parquet | COSINE | 1536 | 5,000,000 | 1,000 | same  as 👆🏻 |
| openai_large_5m_filter99 | Parquet | COSINE | 1536 | 5,000,000 | 1,000 | same  as 👆🏻 |
| laion_large_100m | Parquet | L2 | 768 | 100,000,000 | 1,000 | aws s3 cp s3://assets.zilliz.com/benchmark/laion_large_100m/ ./laion_large_100m/ --region us-west-2 --recursive --no-sign-request |

## Build from source

### Prerequisite

CMake 3.25 or newer, Ninja, and a C++20 compiler are required. If `sccache` or
`ccache` is available, CMake uses it automatically; `sccache` takes precedence.

**MacOS**

```
brew install cmake git libpq ninja sccache
```

**Debian**

```
sudo apt update
sudo apt install -y cmake g++ git libpq-dev ninja-build sccache
```

```
cmake --preset default
cmake --build --preset default
ctest --preset default
```

CMake downloads and builds pinned Arrow, Parquet, argparse, concurrentqueue,
spdlog, Ryu, and nlohmann/json sources inside the build directory. Release
archives are verified with SHA256 checksums. Git submodules and system Arrow packages are not
required. Dependency versions, source URLs, commit IDs, and checksums are
maintained in `cmake/ThirdParty.cmake`; edit a URL there when a different source
is required.

With `BUILD_TESTING=ON` (the default), CMake also downloads and builds pinned
GoogleTest sources. C++ tests use GoogleTest and are discovered automatically by
CTest; the CLI smoke tests run directly through CTest. Configure with
`-DBUILD_TESTING=OFF` to omit tests and the GoogleTest dependency.

The database test is skipped unless `PGVECTORBENCH_TEST_DATABASE` is set. To run
it against a local PostgreSQL instance, use
`PGVECTORBENCH_TEST_DATABASE=postgres ctest --preset default`. Standard libpq
environment variables such as `PGHOST`, `PGPORT`, and `PGUSER` control the
connection.

To use system-installed Arrow and Parquet instead, configure with
`-DPGVECTORBENCH_USE_SYSTEM_ARROW=ON`. If Arrow also imports nlohmann/json, that
dependency is reused instead of downloading the pinned copy. For example, on Debian:

```
wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y libparquet-dev
cmake --preset system-arrow
cmake --build --preset system-arrow
```

## Build docker image

```
docker build -t pgvectorbench .
```

## Usage

```
./pgvectorbench --help
Usage: pgvectorbench [--help] [--version] [--host VAR] [--port VAR] [--username VAR] [--password VAR] [--dbname VAR] [--dataset VAR] [--path VAR] [--log VAR] [--setup VAR] [--load VAR] [--index VAR] [--query VAR] [--teardown VAR]

Optional arguments:
  -h, --help      shows help message and exits 
  -v, --version   prints version information and exits 
  -h, --host      database server host or socket directory 
  -p, --port      database server port 
  -U, --username  database user name 
  -W, --password  password for the specified user 
  -d, --dbname    database name to connect to 
  -D, --dataset   dataset name used to run the benchmark [nargs=0..1] [default: "siftsmall"]
  -P, --path      dataset path 
  -l, --log       send log to file 
  --setup         k/v pairs seperated by semicolon for setup options [nargs=0..1] [default: ""]
  --load          k/v pairs seperated by semicolon for loading dataset [nargs=0..1] [default: ""]
  --index         k/v pairs seperated by semicolon for creating index 
  --query         k/v pairs seperated by semicolon for running the benchmarking queries [nargs=0..1] [default: ""]
  --teardown      k/v pairs seperated by semicolon for teardown options [nargs=0..1] [default: ""]
```

All parameters for the five phases must be specified as `key=value` pairs, with semicolons used to separate each pair. When supplying multiple `key=value` pairs, the entire parameter list should be enclosed in double quotes.

If you organize your dataset with the following structure and place it in the `/opt/datasets` directory, you can omit the `--path` option in all subsequent pgvectorbench commands.

```
➜  datasets tree -L 2
.
├── parquet
│   ├── cohere_medium_1m
│   ├── cohere_small_100k
│   └── openai_small_50k
└── vecs
    ├── crawl
    ├── gist
    ├── glove-100
    ├── sift
    └── siftsmall
```

For instance, if you intend to execute a comprehensive test utilizing the `siftsmall` dataset, you would proceed as follows:

```
./pgvectorbench -d postgres --path /home/zhjwpku/datasets/vecs/siftsmall --setup --load --index="index_type=hnsw;m=32;ef_construction=200" --query="loop=10;hnsw.ef_search=100;percentages=90,99,99.5,99.9"
```

As the previous command did not specify any `teardown` options, you have the flexibility to schedule another query round, potentially with a different setting for `hnsw.ef_search`:

```
./pgvectorbench -d postgres --path /home/zhjwpku/datasets/vecs/siftsmall --query="loop=10;hnsw.ef_search=200;percentages=90,99,99.5,99.9"
```

After benchmarking, you have the option to drop index individually during the teardown phase by executing the following command:

```
./pgvectorbench -d postgres --path /home/zhjwpku/datasets/vecs/siftsmall --teardown=drop_index=y
```

And the  and potentially run another round of query
Subsequently, you create another index and potentially followed by initiating another series of queries to further measure the database's performance:

```
./pgvectorbench -d postgres --path /home/zhjwpku/datasets/vecs/siftsmall --index="maintenance_work_mem=2GB;index_type=hnsw;m=64;ef_construction=200" --query="loop=10;hnsw.ef_search=200;percentages=90,99,99.5,99.9"
```

Prior to initiating the actual benchmarking process, one can prewarm the database by either omitting the `loop` parameter or setting its value to 1:

```
./pgvectorbench -d postgres --path /home/zhjwpku/datasets/vecs/siftsmall --query="loop=1;hnsw.ef_search=200;percentages=90,99,99.5,99.9"
./pgvectorbench -d postgres --path /home/zhjwpku/datasets/vecs/siftsmall --query="loop=10;hnsw.ef_search=200;percentages=90,99,99.5,99.9"
```

As shown by previous examples, pgvectorbench, through the combination of its five phases, is capable of executing a diverse range of performance tests, which is why I consider pgvectorbench to be highly flexible.

There are additional parameters that can be configured for each phase, including but not limited to `thread_num`, `batch_size`, and `table_name`. For an exhaustive list, I recommend referring to the source file.

Query options `k1`, `k2`, `thread_num`, and `loop` must be positive integers,
with `k1 <= k2 <=` the dataset's ground-truth neighbor count. Percentages must
be finite numbers in `[0, 100]`. Invalid input or failed SQL/SET commands abort
the benchmark without reporting query statistics.

Parquet query and ground-truth files must each contain every query ID exactly
once, from `0` to the configured query count minus one. Row order may differ
between files; rows are matched by ID. Embeddings must have the configured
dimension, and each ground-truth row must provide at least `k1` neighbors.

### JSON results

Add `--json` to a command containing `--query` to write a single JSON document
to stdout. Diagnostics and the usual text statistics go to stderr, or to the
file specified by `--log`.

```sh
./pgvectorbench -d postgres --path /opt/datasets/vecs/siftsmall \
  --query="thread_num=8;loop=10;k1=10;k2=10;hnsw.ef_search=100;percentages=50,95,99" \
  --json > result.json

jq '.query | {qps, elapsed_seconds, latency_us, recall}' result.json
```

The table must already be loaded, or the command can include `--setup`,
`--load`, and `--index` as usual. JSON is emitted only after **all requested
phases succeed**, including teardown. Invalid arguments, dataset errors, and
failed SQL/SET commands exit nonzero without emitting a JSON result. Check the
exit code before consuming the output file; failed runs may leave an empty
file when stdout is redirected. `--json` without `--query` is an error.

The versioned result has these fields:

| Field | Meaning |
|-------|---------|
| `schema_version` | JSON schema version, currently `1` |
| `tool_version` | Version of pgvectorbench that produced the result |
| `status` | `"success"`; unsuccessful runs do not produce a result |
| `query.dataset` | Dataset name, format, metric, dimensions, base/query vector counts, and ground-truth neighbor count |
| `query.config` | Resolved table/column, `k1`, `k2`, `thread_num`, `loop`, and explicit `session_overrides` |
| `query.elapsed_seconds` | Query worker wall time used to calculate QPS |
| `query.qps` | Completed queries divided by query worker wall time |
| `query.latency_us` | Latency distribution in microseconds: `count`, `best`, `worst`, `average`, and `percentiles` |
| `query.recall` | Recall `k1@k2` distribution in `[0,1]`, with the same fields |

Each percentile is an object such as `{"percentage":99.5,"value":1234}`.
Percentiles follow the supplied order, including repeated percentages; the
array is empty when `percentages` is omitted. The existing ranking is preserved:
latency percentiles run from fastest to slowest, and recall percentiles run
from highest to lowest recall.

Latency `count` is `query_vectors × loop`; recall `count` is `query_vectors`,
because recall uses only each query's final iteration. Dataset counts come from
the dataset definition, not a database inspection. `session_overrides` contains
only explicitly applied `hnsw.ef_search` and `ivfflat.probes` values; omitted
server defaults are not inferred. Connection credentials and query vectors are
not included in the result.

JSON uses the same computed `QueryResult` as the text statistics, with full
floating-point precision. The timing methodology is unchanged: query worker
wall time includes thread startup, session SET commands, and completion, but
excludes dataset preparation, connection creation, and recall aggregation.
Individual latency includes the database round trip and result handling.

### docker

If you are using docker, you should mount the host's datasets directory to the container's `/opt/datasets` path and also specify the host for the PostgreSQL server.

```
docker run -it --mount type=bind,source=/home/zhjwpku/datasets,target=/opt/datasets pgvectorbench -h 192.168.31.32 -U zhjwpku -d postgres --query="loop=10;hnsw.ef_search=100;percentages=90,99,99.9"
```
