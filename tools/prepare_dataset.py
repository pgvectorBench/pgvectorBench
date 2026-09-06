#!/usr/bin/env python3
"""Download/convert benchmark datasets. No PostgreSQL connection is needed.

The manifest is published last, after source and output validation. Original
vectors and ground truth are never silently pruned, truncated, or reinterpreted.
"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import urllib.request

import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.sparse import csr_matrix

ANN_COMMIT = "2e081ad32c1eccab72dcb739ad886c310b90f715"
ZILLIZ_COMMIT = "24863376eceab8e0fdaa2c39c5cae5e78c969ed4"
BIGANN_COMMIT = "89a3abaafa63dda46b94b308bdf039e699841b3b"
SOURCES = {
    "sift-256-hamming": {
        "url": "https://ann-benchmarks.com/sift-256-hamming.hdf5",
        "definition": f"https://github.com/erikbern/ann-benchmarks/blob/{ANN_COMMIT}/ann_benchmarks/datasets.py",
        "license": "https://ann-benchmarks.com/", "kind": "hdf5",
    },
    "kosarak-jaccard": {
        "url": "https://ann-benchmarks.com/kosarak-jaccard.hdf5",
        "definition": f"https://github.com/erikbern/ann-benchmarks/blob/{ANN_COMMIT}/ann_benchmarks/datasets.py",
        "license": "http://fimi.uantwerpen.be/data/", "kind": "hdf5",
    },
}


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download(url, path):
    """Cache completed downloads and verify their recorded digest on reuse."""
    path = Path(path)
    receipt = path.with_suffix(path.suffix + ".download.json")
    if path.exists():
        if not receipt.exists():
            raise ValueError(f"unverified cached file {path}; use --input for local files")
        info = json.loads(receipt.read_text())
        if info["url"] != url or info["sha256"] != sha256(path):
            raise ValueError(f"cached source changed: {path}")
        return info
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "pgvectorBench/0.1"})
        with urllib.request.urlopen(request, timeout=60) as response, partial.open("wb") as out:
            shutil.copyfileobj(response, out)
            final_url = response.url
        info = {"url": url, "resolved_url": final_url, "sha256": sha256(partial), "bytes": partial.stat().st_size}
        partial.replace(path)
        receipt.write_text(json.dumps(info, indent=2) + "\n")
        return info
    finally:
        partial.unlink(missing_ok=True)


def read_csr(path):
    path = Path(path)
    if path.suffix == ".gz":
        # Keep user-supplied sources read-only. The returned NumPy mappings own
        # the temporary file's mappings after its file handle is closed.
        with gzip.open(path, "rb") as src, tempfile.TemporaryFile() as dst:
            shutil.copyfileobj(src, dst)
            dst.flush()
            dst.seek(0)
            return read_csr_stream(dst)
    with path.open("rb") as src:
        return read_csr_stream(src)


def read_csr_stream(src):
    header = np.fromfile(src, dtype="<i8", count=3)
    if len(header) != 3 or np.any(header < 0):
        raise ValueError("invalid CSR header")
    rows, dim, nnz = map(int, header)
    if rows == 0 or dim == 0 or src.seek(0, os.SEEK_END) != 24 + (rows + 1) * 8 + nnz * 8:
        raise ValueError("invalid CSR dimensions or file length")
    ptr = np.memmap(src, mode="r", dtype="<i8", offset=24, shape=rows + 1)
    indices = np.memmap(src, mode="r", dtype="<i4", offset=24 + (rows + 1) * 8, shape=nnz)
    values = np.memmap(src, mode="r", dtype="<f4", offset=24 + (rows + 1) * 8 + nnz * 4, shape=nnz)
    if ptr[0] != 0 or ptr[-1] != nnz or np.any(np.diff(ptr) < 0) or np.any(indices < 0) or np.any(indices >= dim):
        raise ValueError("invalid CSR offsets or indices")
    matrix = csr_matrix((values, indices, ptr), shape=(rows, dim), copy=False)
    if not matrix.has_canonical_format:
        raise ValueError("CSR indices must be sorted and unique")
    if not np.all(np.isfinite(values)) or np.any(values == 0):
        raise ValueError("CSR values must be finite and nonzero")
    return matrix


def read_bigann_gt(path, rows, base_count):
    with Path(path).open("rb") as src:
        header = np.fromfile(src, dtype="<u4", count=2)
        if len(header) != 2 or header[0] != rows or header[1] == 0:
            raise ValueError("invalid BigANN ground truth header")
        width = int(header[1])
        expected = 8 + rows * width * 8  # uint32 IDs, then float32 distances
        if Path(path).stat().st_size != expected:
            raise ValueError("invalid BigANN ground truth length")
        neighbors = np.fromfile(src, dtype="<u4", count=rows * width).reshape(rows, width).astype(np.int64)
    validate_neighbors(neighbors, base_count)
    return neighbors


def validate_neighbors(neighbors, base_count):
    if neighbors.ndim != 2 or min(neighbors.shape) == 0 or not np.issubdtype(neighbors.dtype, np.integer):
        raise ValueError("ground truth must be a nonempty integer matrix")
    if np.any(neighbors < 0) or np.any(neighbors >= base_count):
        raise ValueError("ground truth neighbor out of range")
    for row in neighbors:
        if len(np.unique(row)) != len(row):
            raise ValueError("duplicate ground truth neighbor")


def exact_neighbors(base, queries, metric, k):
    """Small fixtures/derived L1 sets only. Deterministic distance, then ID order."""
    result = []
    for q in queries:
        if metric == "l1":
            distances = np.abs(base - q).sum(axis=1)
        elif metric == "l2":
            distances = np.square(base - q).sum(axis=1)
        elif metric == "ip":
            distances = -(base @ q)
        elif metric == "cosine":
            distances = 1 - (base @ q) / (np.linalg.norm(base, axis=1) * np.linalg.norm(q))
        elif metric == "hamming":
            distances = (base != q).sum(axis=1)
        elif metric == "jaccard":
            intersection = np.logical_and(base, q).sum(axis=1)
            union = np.logical_or(base, q).sum(axis=1)
            distances = 1 - np.divide(intersection, union, out=np.ones_like(union, dtype=float), where=union != 0)
        else:
            raise ValueError(f"unsupported metric {metric}")
        if not np.isfinite(distances).all():
            raise ValueError("non-finite exact distances")
        result.append(np.lexsort((np.arange(len(base)), distances))[:k])
    return np.asarray(result, dtype=np.int64)


def write_vectors(path, data, vector_type, dim, batch_size=2048):
    if vector_type == "sparsevec":
        emb_type = pa.struct([("indices", pa.list_(pa.int32())), ("values", pa.list_(pa.float32()))])
    elif vector_type == "bit":
        emb_type = pa.string()
    else:
        emb_type = pa.list_(pa.float32())
    schema = pa.schema([("id", pa.int64()), ("emb", emb_type)])
    count = data.shape[0] if hasattr(data, "shape") else len(data)
    max_nnz = 0
    with pq.ParquetWriter(path, schema, compression="zstd") as writer:
        for start in range(0, count, batch_size):
            block = data[start:start + batch_size]
            if vector_type == "sparsevec":
                nnz = np.diff(block.indptr)
                max_nnz = max(max_nnz, int(nnz.max(initial=0)))
                if max_nnz > 16000:
                    raise ValueError("sparse row exceeds pgvector storage limit of 16000 NNZ")
                embeddings = [{"indices": block.indices[block.indptr[i]:block.indptr[i+1]].tolist(),
                               "values": block.data[block.indptr[i]:block.indptr[i+1]].tolist()} for i in range(block.shape[0])]
            elif vector_type == "bit":
                embeddings = []
                for row in block:
                    if len(row) != dim or not np.isin(row, [0, 1]).all():
                        raise ValueError("invalid binary embedding")
                    embeddings.append("".join("1" if x else "0" for x in row))
            else:
                block = np.asarray(block, dtype=np.float32)
                if block.ndim != 2 or block.shape[1] != dim or not np.isfinite(block).all():
                    raise ValueError("invalid dense embedding")
                embeddings = block.tolist()
            writer.write_table(pa.Table.from_arrays([
                pa.array(range(start, start + len(embeddings)), type=pa.int64()),
                pa.array(embeddings, type=emb_type)], schema=schema))
    return count, max_nnz


def publish(out, name, base, queries, neighbors, vector_type, metric, dim, provenance):
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", name):
        raise ValueError("invalid dataset name")
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = out / "dataset.json"
    if manifest_path.exists() or any((out / f).exists() for f in ["train.parquet", "test.parquet", "neighbors.parquet"]):
        raise ValueError("output dataset already exists; choose a new output directory")
    base_count = base.shape[0] if hasattr(base, "shape") else len(base)
    query_count = queries.shape[0] if hasattr(queries, "shape") else len(queries)
    validate_neighbors(neighbors, base_count)
    if query_count != len(neighbors):
        raise ValueError("query and ground truth counts differ")
    if dim <= 0 or (vector_type == "bit" and dim > 64000):
        raise ValueError("invalid dimension or bit index exceeds 64000 dimensions")
    _, max_nnz = write_vectors(out / "train.parquet", base, vector_type, dim)
    write_vectors(out / "test.parquet", queries, vector_type, dim)
    pq.write_table(pa.table({"id": pa.array(range(query_count), type=pa.int64()),
                            "neighbors": pa.array(neighbors.tolist(), type=pa.list_(pa.int64()))}),
                   out / "neighbors.parquet", compression="zstd")
    manifest = {"schema_version": 1, "name": name, "format": "parquet", "vector_type": vector_type,
                "metric": metric, "dimensions": int(dim), "root": ".",
                "base_files": [{"path": "train.parquet", "rows": base_count}],
                "query_file": {"path": "test.parquet", "rows": query_count},
                "ground_truth": {"path": "neighbors.parquet", "rows": query_count, "neighbors": neighbors.shape[1]},
                "provenance": provenance, "max_base_nnz": max_nnz if vector_type == "sparsevec" else None}
    for item in manifest["base_files"] + [manifest["query_file"], manifest["ground_truth"]]:
        path = out / item["path"]
        if pq.ParquetFile(path).metadata.num_rows != item["rows"]:
            raise ValueError("converted row count mismatch")
        item["sha256"] = sha256(path)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    if max_nnz > 1000:
        print(f"WARNING: {max_nnz} max NNZ exceeds HNSW limit 1000; source was not pruned", file=sys.stderr)
    return manifest_path


def convert_hdf5(path, out, name, provenance):
    with h5py.File(path, "r") as source:
        metric = source.attrs["distance"]
        if isinstance(metric, bytes):
            metric = metric.decode()
        if metric not in ("hamming", "jaccard"):
            raise ValueError("this HDF5 adapter expects Hamming or Jaccard ground truth")
        if "dimension" in source.attrs:
            dim = int(source.attrs["dimension"])
        elif metric == "hamming" and source["train"].ndim == 2:
            dim = source["train"].shape[1]
        else:
            raise ValueError("set datasets require an explicit dimension attribute")
        if not 0 < dim <= 64000:
            raise ValueError("bit dimensions must be within 1..64000")
        if metric == "jaccard":
            # ANN-Benchmarks stores concatenated set members plus per-row lengths.
            def sets(key):
                flat = source[key]
                size_key = "size_" + key if "size_" + key in source else key + "_size"
                if size_key not in source:
                    class VariableSets:
                        shape = (len(flat), dim)
                        def __getitem__(self, item):
                            rows = flat[item]
                            block = np.zeros((len(rows), dim), dtype=bool)
                            for i, members in enumerate(rows):
                                indices = np.asarray(members)
                                if not np.issubdtype(indices.dtype, np.integer) or np.any(indices < 0) or np.any(indices >= dim):
                                    raise ValueError("set member outside dimension")
                                block[i, indices] = True
                            return block
                    return VariableSets()
                sizes = np.asarray(source[size_key], dtype=np.int64)
                if np.any(sizes < 0) or sizes.sum() != len(flat):
                    raise ValueError("invalid Jaccard set offsets")
                # Keep sparse sets in memory; construct dense bits only per batch.
                class Sets:
                    shape = (len(sizes), dim)
                    offsets = np.r_[0, np.cumsum(sizes)]
                    def __getitem__(self, item):
                        rows = range(*item.indices(len(sizes)))
                        block = np.zeros((len(rows), dim), dtype=bool)
                        for i, r in enumerate(rows):
                            indices = np.asarray(flat[self.offsets[r]:self.offsets[r+1]])
                            if np.any(indices < 0) or np.any(indices >= dim) or not np.issubdtype(indices.dtype, np.integer):
                                raise ValueError("set member outside dimension")
                            block[i, indices] = True
                        return block
                return Sets()
            base, queries = sets("train"), sets("test")
        else:
            base, queries = source["train"], source["test"]
        return publish(out, name, base, queries, np.asarray(source["neighbors"]), "bit", metric, dim, provenance)



def prepare_zilliz(name, out, cache, input_dir=None):
    specs = {"cohere_small_100k": (100000, 768), "openai_small_50k": (50000, 1536),
             "bioasq_medium_1m": (1000000, 1024)}
    expected_rows, dim = specs[name]
    out.mkdir(parents=True, exist_ok=True)
    if any(out.iterdir()):
        raise ValueError("output directory must be empty")
    receipts = []
    counts = {}
    source_ids = set()
    scalar_type = None
    for filename in ("train.parquet", "test.parquet", "neighbors.parquet"):
        path = input_dir / filename if input_dir else cache / name / filename
        receipts.append({"local_file": str(path), "sha256": sha256(path)} if input_dir else
                        download(f"https://assets.zilliz.com/benchmark/{name}/{filename}", path))
        parquet = pq.ParquetFile(path)
        counts[filename] = parquet.metadata.num_rows
        seen = set()
        writer = None
        try:
            for batch in parquet.iter_batches(batch_size=2048):
                table = pa.Table.from_batches([batch])
                ids = table["id"].cast(pa.int64()).to_pylist()
                if any(i is None or i < 0 or i in seen for i in ids) or len(set(ids)) != len(ids):
                    raise ValueError("invalid or duplicate Parquet IDs")
                seen.update(ids)
                if filename == "neighbors.parquet":
                    neighbors = table.column(1).cast(pa.list_(pa.int64()))
                    rows = neighbors.to_pylist()
                    for row in rows:
                        if not row or len(set(row)) != len(row) or any(i not in source_ids for i in row):
                            raise ValueError("invalid ground truth neighbors")
                    widths = [len(r) for r in rows]
                    counts["neighbors"] = min(counts.get("neighbors", widths[0]), min(widths))
                    columns = {"id": pa.array(ids, type=pa.int64()), "neighbors": neighbors}
                else:
                    emb = table["emb"]
                    element_type = emb.type.value_type
                    if element_type not in (pa.float32(), pa.float64()):
                        raise ValueError("Zilliz embeddings must be float32 or float64")
                    current_type = "float32" if element_type == pa.float32() else "float64"
                    if scalar_type is not None and current_type != scalar_type:
                        raise ValueError("train and query scalar types differ")
                    scalar_type = current_type
                    for row in emb.to_pylist():
                        if row is None or len(row) != dim or not np.isfinite(row).all():
                            raise ValueError("invalid embedding dimension or values")
                    columns = {"id": pa.array(ids, type=pa.int64()), "emb": emb}
                converted = pa.table(columns)
                if writer is None:
                    writer = pq.ParquetWriter(out / filename, converted.schema, compression="zstd")
                writer.write_table(converted)
        finally:
            if writer is not None:
                writer.close()
        if filename == "train.parquet":
            source_ids = seen
        elif seen != set(range(counts[filename])):
            raise ValueError("query/ground truth IDs must cover 0..N-1")
    if counts["train.parquet"] != expected_rows or counts["test.parquet"] != counts["neighbors.parquet"]:
        raise ValueError("Zilliz dataset counts do not match definition")
    manifest = {"schema_version": 1, "name": name, "format": "parquet", "vector_type": "vector",
                "metric": "cosine", "dimensions": dim, "scalar_type": scalar_type,
                "base_files": [{"path": "train.parquet", "rows": expected_rows}],
                "query_file": {"path": "test.parquet", "rows": counts["test.parquet"]},
                "ground_truth": {"path": "neighbors.parquet", "rows": counts["neighbors.parquet"], "neighbors": counts["neighbors"]},
                "provenance": {"sources": receipts, "conversion": "preserve IDs, precision and official neighbors; canonical column order",
                               "definition": f"https://github.com/zilliztech/VectorDBBench/blob/{ZILLIZ_COMMIT}/vectordb_bench/backend/dataset.py",
                               "license": "https://github.com/zilliztech/VectorDBBench#datasets"}}
    for item in manifest["base_files"] + [manifest["query_file"], manifest["ground_truth"]]:
        item["sha256"] = sha256(out / item["path"])
    result = out / "dataset.json"
    result.write_text(json.dumps(manifest, indent=2) + "\n")
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    zilliz = sub.add_parser("zilliz", help="download and validate a small Zilliz dataset, or optional BioASQ 1M")
    zilliz.add_argument("dataset", choices=["cohere_small_100k", "openai_small_50k", "bioasq_medium_1m"])
    zilliz.add_argument("--input-dir", type=Path)
    zilliz.add_argument("--output", type=Path, required=True)
    zilliz.add_argument("--cache", type=Path, default=Path("build/datasets-cache"))
    ann = sub.add_parser("ann", help="convert a local HDF5 or download a named ANN-Benchmarks dataset")
    ann.add_argument("dataset", choices=SOURCES)
    ann.add_argument("--input", type=Path)
    ann.add_argument("--output", type=Path, required=True)
    ann.add_argument("--cache", type=Path, default=Path("build/datasets-cache"))
    sparse = sub.add_parser("splade", help="official BigANN small/1M subset with matched IP ground truth")
    sparse.add_argument("--size", choices=["small", "1M"], default="small")
    sparse.add_argument("--input-dir", type=Path)
    sparse.add_argument("--output", type=Path, required=True)
    sparse.add_argument("--cache", type=Path, default=Path("build/datasets-cache"))
    l1 = sub.add_parser("siftsmall-l1", help="derive L1 ground truth from the original siftsmall fvecs")
    l1.add_argument("--input-dir", type=Path, required=True)
    l1.add_argument("--output", type=Path, required=True)
    fixture = sub.add_parser("fixture", help="generate deterministic correctness data, not a real-world benchmark")
    fixture.add_argument("--type", choices=["vector", "bit", "sparsevec"], default="vector")
    fixture.add_argument("--metric", choices=["l1", "l2", "ip", "cosine", "hamming", "jaccard"], default="l2")
    fixture.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "zilliz":
        result = prepare_zilliz(args.dataset, args.output, args.cache, args.input_dir)
    elif args.command == "ann":
        spec = SOURCES[args.dataset]
        path = args.input or args.cache / (args.dataset + ".hdf5")
        receipt = {"local_file": str(path), "sha256": sha256(path)} if args.input else download(spec["url"], path)
        result = convert_hdf5(path, args.output, args.dataset.replace("-", "_"), {**spec, "source": receipt, "conversion": "preserve IDs, bits and official ground truth"})
    elif args.command == "splade":
        names = [f"base_{args.size}.csr.gz", "queries.dev.csr.gz", f"base_{args.size}.dev.gt"]
        root = args.input_dir or args.cache / "splade"
        receipts = []
        for name in names:
            path = root / name
            receipts.append({"local_file": str(path), "sha256": sha256(path)} if args.input_dir else
                            download("https://storage.googleapis.com/ann-challenge-sparse-vectors/csr/" + name, path))
        base, queries = read_csr(root / names[0]), read_csr(root / names[1])
        if base.shape[0] != (100000 if args.size == "small" else 1000000) or base.shape[1] != queries.shape[1]:
            raise ValueError("SPLADE dimensions or subset size mismatch")
        gt = read_bigann_gt(root / names[2], queries.shape[0], base.shape[0])
        result = publish(args.output, "splade_" + args.size.lower(), base, queries, gt, "sparsevec", "ip", base.shape[1],
                         {"definition": f"https://github.com/harsha-simhadri/big-ann-benchmarks/blob/{BIGANN_COMMIT}/benchmark/datasets.py",
                          "license": "https://big-ann-benchmarks.com/", "sources": receipts, "conversion": "no pruning; original subset GT"})
    elif args.command == "siftsmall-l1":
        def fvecs(name, rows):
            words = np.fromfile(args.input_dir / name, dtype="<i4")
            if words.size != rows * 129:
                raise ValueError("expected original siftsmall row count and 128 dimensions")
            words = words.reshape(rows, 129)
            if not np.all(words[:, 0] == 128):
                raise ValueError("invalid fvecs dimension")
            return words[:, 1:].copy().view("<f4")
        base = fvecs("siftsmall_base.fvecs", 10000)
        queries = fvecs("siftsmall_query.fvecs", 100)
        result = publish(args.output, "siftsmall_l1", base, queries, exact_neighbors(base, queries, "l1", 100), "vector", "l1", 128,
                         {"source": "http://corpus-texmex.irisa.fr/", "license": "http://corpus-texmex.irisa.fr/",
                          "source_sha256": {n: sha256(args.input_dir / n) for n in ["siftsmall_base.fvecs", "siftsmall_query.fvecs"]},
                          "ground_truth": "recomputed exact L1, tie break by ID"})
    else:
        rng = np.random.default_rng(20260905)
        base = rng.normal(size=(256, 16)).astype(np.float32)
        queries = rng.normal(size=(12, 16)).astype(np.float32)
        if args.type == "bit":
            if args.metric not in ("hamming", "jaccard"):
                parser.error("bit fixtures require hamming or jaccard")
            base, queries = base > 0, queries > 0
        elif args.metric in ("hamming", "jaccard"):
            parser.error("binary metrics require bit fixtures")
        if args.type == "sparsevec":
            base[abs(base) < 0.8] = 0
            queries[abs(queries) < 0.8] = 0
        gt = exact_neighbors(base, queries, args.metric, 100)
        if args.type == "sparsevec":
            base, queries = csr_matrix(base), csr_matrix(queries)
        result = publish(args.output, f"fixture_{args.type}_{args.metric}", base, queries, gt, args.type, args.metric, 16,
                         {"synthetic": True, "seed": 20260905, "ground_truth": "exact distance, tie break by ID"})
    print(result)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        sys.exit(f"dataset preparation failed: {error}")
