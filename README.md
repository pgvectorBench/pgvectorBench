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

**MacOS**

```
brew install apache-arrow
brew install libpq
```

**Debian**

```
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y libparquet-dev libpq-dev
```

```
git submodule update --init --recursive
mkdir build && cd build
cmake .. && make -j
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

### docker

If you are using docker, you should mount the host's datasets directory to the container's `/opt/datasets` path and also specify the host for the PostgreSQL server.

```
docker run -it --mount type=bind,source=/home/zhjwpku/datasets,target=/opt/datasets pgvectorbench -- -h 192.168.31.32 -U zhjwpku -d postgres --query="loop=10;hnsw.ef_search=100;percentages=90,99,99.9"
```
