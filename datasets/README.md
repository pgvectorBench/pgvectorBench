# Additional dataset and index benchmarks

These tools extend pgvectorBench to pgvector's native halfvec, bit, sparsevec,
and half-precision/binary expression indexes. They do not install other database
extensions. pgvector 0.8.1 remains the CI baseline; local validation uses 0.8.6.

## Prepare data

Python is optional and is not part of the C++ build. The pinned tool dependencies
require Python 3.12 or newer:

```sh
python3 -m venv build/data-tools-venv
build/data-tools-venv/bin/pip install -r tools/requirements.txt

build/data-tools-venv/bin/python tools/prepare_dataset.py splade \
  --output build/datasets/splade_small
build/data-tools-venv/bin/python tools/prepare_dataset.py ann kosarak-jaccard \
  --output build/datasets/kosarak_jaccard
build/data-tools-venv/bin/python tools/prepare_dataset.py ann sift-256-hamming \
  --output build/datasets/sift_256_hamming
build/data-tools-venv/bin/python tools/prepare_dataset.py zilliz cohere_small_100k \
  --output build/datasets/cohere_small_100k
build/data-tools-venv/bin/python tools/prepare_dataset.py zilliz openai_small_50k \
  --output build/datasets/openai_small_50k
```

Each command publishes `dataset.json` after validating and converting all files.
Use a new output directory. Incomplete conversions leave files for inspection;
remove that incomplete output or choose a new directory before retrying. Completed
source downloads are cached in `build/datasets-cache`, with source URL, byte count,
and SHA256 receipts checked on reuse. Upstream definitions are pinned to commits
in `prepare_dataset.py`; generated manifests record source and output hashes.
Checksums recorded at first download are provenance, not independently verified
upstream release checksums. The C++ hot path checks structure/counts, not hashes.

For existing files, `ann --input FILE.hdf5`, `splade --input-dir DIR`, and
`zilliz --input-dir DIR` avoid downloading. SPLADE expects the original compressed
CSR files and matching `.dev.gt`. Existing built-in dataset names remain valid;
Zilliz conversion is useful for generating a manifest or canonicalizing columns.
Optional larger cases are `splade --size 1M` and `zilliz bioasq_medium_1m`.

| Dataset | Metric | Observed size / dimensions | Purpose |
|---|---|---|---|
| Cohere small | Cosine | 100,000 × 768 | vector, halfvec, quantization/retrieval |
| OpenAI small | Cosine | 50,000 × 1,536 | higher-dimensional quantization |
| SIFT-Hamming | Hamming | 988,258 × 256; 1,000 queries | native bit HNSW/IVFFlat |
| Kosarak | Jaccard | 74,962 × 41,260; 500 queries | native bit HNSW |
| SPLADE small | IP | 100,000 × 30,109; 6,980 queries | sparsevec HNSW |
| SIFT-small (derived ground truth) | L1 | 10,000 × 128; 100 queries | L1 HNSW and quantization |
| BioASQ medium (optional) | Cosine | 1,000,000 × 1,024 | larger dense case |

The observed ANN-Benchmarks file metadata can differ from its README table.
The converters use the actual dimension/shape, preserving IDs and official
neighbors. Set-valued Jaccard HDF5 data supports concatenated members with sizes
and variable-length rows; sets are converted to bit strings in bounded batches.
Sources and original data terms:
[ANN-Benchmarks](https://github.com/erikbern/ann-benchmarks),
[Kosarak](http://fimi.uantwerpen.be/data/),
[BigANN/SPLADE](https://big-ann-benchmarks.com/), and
[VectorDBBench](https://github.com/zilliztech/VectorDBBench).
The benchmark repositories' software licenses do not replace dataset terms.

SPLADE is not pruned. The manifest records maximum base NNZ; native sparsevec
storage allows 16,000 NNZ, but HNSW requires at most 1,000. Index creation checks
stored rows before issuing CREATE INDEX and reports an explicit error on excess.
The downloaded 100K subset's observed maximum is 301. Duplicate sparse
indices, invalid indices, and non-finite/zero values are rejected;
empty sparse vectors are supported by the format. No dense expansion is used.

L1 needs its own ground truth:

```sh
build/data-tools-venv/bin/python tools/prepare_dataset.py siftsmall-l1 \
  --input-dir /data/siftsmall --output build/datasets/siftsmall_l1
```

This requires the original 10,000 base and 100 query vectors (128 dimensions).
It computes exact L1 top-100 with deterministic ID tie-breaking. Never reuse L2
neighbors for L1 or full-dataset neighbors for a smaller base subset. Quantization
experiments intentionally use original floating-point neighbors to measure
end-to-end quality; half precision may change ranking. ID-based recall is retained;
Hamming/Jaccard distances can tie at K, so equally good alternative IDs may reduce
reported recall.

## Manifest and Parquet contract

```json
{
  "schema_version": 1,
  "name": "custom_bits",
  "format": "parquet",
  "vector_type": "bit",
  "metric": "hamming",
  "dimensions": 256,
  "root": ".",
  "base_files": [{"path": "train.parquet", "rows": 100000}],
  "query_file": {"path": "test.parquet", "rows": 1000},
  "ground_truth": {"path": "neighbors.parquet", "rows": 1000, "neighbors": 100}
}
```

`--dataset-config FILE` is exclusive with an explicitly supplied `--dataset`.
`root` defaults to the manifest directory; `--path` overrides the root. File paths
are relative to the root. Counts must be positive and query/ground-truth counts
must match. Names must be simple lowercase SQL identifiers of at most 63 bytes.
Optional `scalar_type` is `float32` (default) or `float64` for Parquet. `format`
also accepts `fvecs` and `bvecs` for dense sources. Byte VECS remain numeric byte
vectors, not packed bits. Filtered built-ins retain their existing behavior;
custom manifests do not accept raw filter SQL.

Use Zstandard (the converters’ default) or uncompressed Parquet; the bundled
Arrow build does not include Snappy. Canonical Parquet column order:

| File / representation | Columns |
|---|---|
| Dense train/query | `id: int64`, `emb: list<float32>` (or float64) |
| Bit train/query | `id: int64`, `emb: string` of exactly D binary digits |
| Sparse train/query | `id: int64`, `emb: struct<indices: list<int32>, values: list<float32>>` (or float64) |
| Ground truth | `id: int64`, `neighbors: list<int64>` |

Sparse indices in Parquet are sorted, unique, zero-based, and in range; conversion
to pgvector's one-based `{index:value}/dimensions` happens only during COPY/query
formatting. Values must be finite and nonzero. Query and ground-truth IDs cover
`0..N-1` exactly once; row order may differ. Native bit datasets require binary
metrics, and dense/sparse datasets require L1/L2/IP/Cosine. Storage conversion is
limited to vector ↔ halfvec; quantization is an index representation, not an
implicit reinterpretation of the source dataset.

```sh
./build/pgvectorbench -d bench --dataset-config build/datasets/splade_small/dataset.json \
  --setup --load --index='index_type=hnsw;m=16;ef_construction=64' \
  --query='k1=10;k2=10;thread_num=1;hnsw.ef_search=100;explain=true' --json
```

For independent phases, repeat `--storage-type` and `--index-representation`.
Preflight rejects mismatched column types/dimensions. Missing or mismatched
indexes emit a warning, allowing explicit exact-scan baselines. The catalog
expression match does not prove that a query plan uses an index.

## Performance matrix

Run on an existing test database with `CREATE EXTENSION vector`, specifying
libpq connection variables as needed:

```sh
build/data-tools-venv/bin/python tools/run_matrix.py \
  --database bench --dataset-config build/datasets/cohere_small_100k/dataset.json \
  --output build/results/cohere
```

The runner creates a unique schema, gives each configuration its own table,
loads the data, runs ANALYZE, builds one index, warms up once, and measures three
query loops. Cases run serially. It drops only the schema and tables it created,
including on failures. Credentials stay in libpq environment variables. No
server-wide settings are changed. Use a fresh result directory per run.

Defaults: `k1=k2=10`, one query thread, HNSW `m=16, ef_construction=64` and
`ef_search=40/100/200`; IVFFlat `lists=100` and `probes=1/10/50`. Dense vector
sources run native vector, native halfvec, halfvec expression, and binary
expression variants. Expression variants include direct search and original-vector
reranking with candidate sizes 100/200. Unsupported index combinations are skipped.
`--method`, `--storage-type`, and `--representation` select a subset of the matrix.
Use `--maintenance-work-mem=512MB` to set build-session memory explicitly.
Other database settings retain their server defaults and are recorded in results.

Results include load throughput, build time, table/index sizes, QPS, latency
P50/P95/P99, recall, and effective settings. Each measured result includes the
first query's EXPLAIN JSON captured outside timing with matching search settings.
The opt-in plan includes PostgreSQL expressions, which may contain query vectors.
The runner supplies `require_index=INDEX_NAME` to check that sample before
warmup and measurement. A mismatched plan is logged, the parameter combination
is labeled `planner_did_not_use_target_index`, and measurement is skipped.
The matrix exits nonzero if any case fails or is skipped; this is a sample,
not an all-query execution-plan guarantee. It does not force index use for performance measurements.
Failed profiles record an error and log; remaining profiles still run.

## Correctness tests

```sh
cmake --build --preset default
PGVECTORBENCH_TEST_DATABASE=bench ctest --preset default
PGVECTORBENCH_TEST_DATABASE=bench build/data-tools-venv/bin/python \
  -m unittest discover -s tools -p 'test_*.py' -v
```

The Python suite synthesizes tiny deterministic datasets, tests actual COPY/index/
query behavior for every legal combination, reranking against exact neighbors,
HDF5/CSR conversion, invalid input, and sparse/halfvec limits. It forces index use
only when checking index plans on tiny fixtures. No real dataset downloads are
needed in CI. Without the test database variable, database tests are skipped.
Fixture generation is also available via `prepare_dataset.py fixture --type bit
--metric hamming --output DIR`; fixtures are not real-world performance evidence.
