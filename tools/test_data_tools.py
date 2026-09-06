import gzip
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import uuid

import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from psycopg import sql
from scipy.sparse import csr_matrix

from prepare_dataset import (convert_hdf5, exact_neighbors, publish, read_csr,
                             read_bigann_gt, sha256, validate_neighbors)
from run_matrix import contains_index, profiles

ROOT = Path(__file__).resolve().parents[1]
BINARY = Path(os.environ.get("PGVECTORBENCH_BINARY", ROOT / "build/pgvectorbench"))


class TemporaryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()


class ConversionTest(TemporaryTest):
    def test_hdf5_bits_preserve_ids_and_ground_truth(self):
        path = self.root / "bits.hdf5"
        with h5py.File(path, "w") as f:
            f.attrs.update(dimension=3, distance="hamming")
            f["train"] = np.array([[0, 1, 0], [1, 0, 1]], dtype=bool)
            f["test"] = np.array([[1, 0, 1]], dtype=bool)
            f["neighbors"] = np.array([[1, 0]], dtype=np.int32)
        output = convert_hdf5(path, self.root / "converted", "bits", {})
        manifest = json.loads(output.read_text())
        self.assertEqual(pq.read_table(output.parent / "train.parquet")["emb"].to_pylist(), ["010", "101"])
        self.assertEqual(pq.read_table(output.parent / "neighbors.parquet")["neighbors"].to_pylist(), [[1, 0]])
        self.assertEqual(manifest["base_files"][0]["sha256"], sha256(output.parent / "train.parquet"))

    def test_hdf5_set_offsets_use_declared_universe(self):
        path = self.root / "sets.hdf5"
        with h5py.File(path, "w") as f:
            f.attrs.update(dimension=7, distance="jaccard")
            f["train"] = [0, 6, 1]
            f["size_train"] = [2, 1]
            f["test"] = [1]
            f["size_test"] = [1]
            f["neighbors"] = np.array([[1, 0]], dtype=np.int32)
        output = convert_hdf5(path, self.root / "converted", "sets", {})
        self.assertEqual(pq.read_table(output.parent / "train.parquet")["emb"].to_pylist(), ["1000001", "0100000"])

    def test_csr_and_ground_truth_binary_layout_and_corruption(self):
        path = self.root / "test.csr"
        with path.open("wb") as f:
            f.write(np.array([2, 30000, 3], dtype="<i8").tobytes())
            f.write(np.array([0, 2, 3], dtype="<i8").tobytes())
            f.write(np.array([0, 29999, 2], dtype="<i4").tobytes())
            f.write(np.array([1, 2, 3], dtype="<f4").tobytes())
        matrix = read_csr(path)
        self.assertEqual(matrix.shape, (2, 30000))
        self.assertEqual(matrix[0, 29999], 2)
        compressed = path.with_suffix(".csr.gz")
        compressed.write_bytes(gzip.compress(path.read_bytes()))
        original = path.read_bytes()
        path.write_bytes(b"existing uncompressed file must stay unchanged")
        mapped = read_csr(compressed)
        self.assertEqual(mapped[0, 29999], 2)
        self.assertEqual(path.read_bytes(), b"existing uncompressed file must stay unchanged")
        path.write_bytes(original)
        gt = self.root / "test.gt"
        with gt.open("wb") as f:
            f.write(np.array([1, 2, 1, 0], dtype="<u4").tobytes())
            f.write(np.array([0, 1], dtype="<f4").tobytes())
        self.assertEqual(read_bigann_gt(gt, 1, 2).tolist(), [[1, 0]])
        with path.open("ab") as f:
            f.write(b"bad")
        with self.assertRaises(ValueError):
            read_csr(path)
        with self.assertRaises(ValueError):
            validate_neighbors(np.array([[0, 0]]), 2)
        with self.assertRaises(ValueError):
            read_bigann_gt(gt, 2, 2)

    def test_l1_ground_truth_is_recomputed(self):
        base = np.array([[3, 0], [2, 2]], dtype=np.float32)
        queries = np.array([[0, 0]], dtype=np.float32)
        self.assertEqual(exact_neighbors(base, queries, "l1", 1).tolist(), [[0]])
        self.assertEqual(exact_neighbors(base, queries, "l2", 1).tolist(), [[1]])

    def test_sparse_publishing_does_not_prune(self):
        base = csr_matrix(np.ones((2, 1001), dtype=np.float32))
        output = publish(self.root / "sparse", "sparse", base, base[:1], np.array([[0]]),
                         "sparsevec", "ip", 1001, {})
        self.assertEqual(json.loads(output.read_text())["max_base_nnz"], 1001)
        self.assertEqual(len(pq.read_table(output.parent / "train.parquet")["emb"][0].as_py()["indices"]), 1001)

    def test_sweep_excludes_unsupported_combinations_and_checks_nested_plans(self):
        self.assertEqual(list(profiles("sparsevec", "ip")), [("sparsevec", "native", "hnsw")])
        self.assertEqual(list(profiles("bit", "jaccard")), [("bit", "native", "hnsw")])
        self.assertTrue(contains_index([{"Plan": {"Plans": [{"Index Name": "ann"}]}}], "ann"))
        self.assertFalse(contains_index([{"Plan": {"Node Type": "Seq Scan"}}], "ann"))


@unittest.skipUnless(os.environ.get("PGVECTORBENCH_TEST_DATABASE"), "set PGVECTORBENCH_TEST_DATABASE for database tests")
class VectorIntegrationTest(TemporaryTest):
    def setUp(self):
        super().setUp()
        self.database = os.environ["PGVECTORBENCH_TEST_DATABASE"]
        self.conn = psycopg.connect(dbname=self.database, autocommit=True)
        self.schema = "pgvbt_" + uuid.uuid4().hex[:12]
        self.conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.counter = 0

    def tearDown(self):
        self.conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))
        self.conn.close()
        super().tearDown()

    def fixture(self, source, metric, rows=256, dim=16):
        self.counter += 1
        rng = np.random.default_rng(429 + self.counter)
        base = rng.normal(size=(rows, dim)).astype(np.float32)
        queries = rng.normal(size=(5, dim)).astype(np.float32)
        if source == "bit":
            base, queries = base > 0, queries > 0
        if source == "sparsevec":
            base[abs(base) < .8] = 0
            queries[abs(queries) < .8] = 0
        gt = exact_neighbors(base, queries, metric, min(100, rows))
        if source == "sparsevec":
            base, queries = csr_matrix(base), csr_matrix(queries)
        return publish(self.root / str(self.counter), f"fixture_{self.counter}", base, queries, gt,
                       source, metric, dim, {})

    def cli(self, manifest, *options, success=True, force_index=False):
        env = os.environ.copy()
        if force_index:
            env["PGOPTIONS"] = env.get("PGOPTIONS", "") + " -c enable_seqscan=off"
        result = subprocess.run([str(BINARY), "--dataset-config", str(manifest), "--dbname", self.database,
                                 "--json", *options], capture_output=True, text=True, env=env, timeout=45)
        if not success:
            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, "")
            return result.stderr
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_all_legal_native_operator_classes_load_index_query(self):
        for source in ("vector", "halfvec", "bit", "sparsevec"):
            for metric in (("hamming", "jaccard") if source == "bit" else ("l1", "l2", "ip", "cosine")):
                for method in ("hnsw", "ivfflat"):
                    if method == "ivfflat" and (source == "sparsevec" or metric in ("l1", "jaccard")):
                        continue
                    with self.subTest(source=source, metric=metric, method=method):
                        manifest = self.fixture("vector" if source == "halfvec" else source, metric)
                        table = f"{self.schema}.t{self.counter}"
                        name = f"ann_{self.counter}"
                        self.cli(manifest, "--storage-type", source, f"--setup=table_name={table}",
                                 f"--load=table_name={table};thread_num=1;client_num=1")
                        self.conn.execute(sql.SQL("ANALYZE {}.{}").format(sql.Identifier(self.schema), sql.Identifier(f"t{self.counter}")))
                        index = self.cli(manifest, "--storage-type", source,
                                         f"--index=table_name={table};index_name={name};index_type={method}" + (";lists=4" if method == "ivfflat" else ""))
                        self.assertEqual(index["index"]["table"]["columns"][1]["dimensions"], 16)
                        query = self.cli(manifest, "--storage-type", source,
                                         f"--query=table_name={table};thread_num=1;k1=1;k2=10;hnsw.ef_search=256;ivfflat.probes=4;explain=true;require_index={name}", force_index=True)
                        self.assertEqual(query["schema_version"], 3)
                        self.assertEqual(query["query"]["table"]["warnings"], [])
                        # All lists is an exact IVFFlat search; HNSW scans all 256 candidates.
                        if source != "bit":
                            self.assertEqual(query["query"]["recall"]["average"], 1)
                        self.assertTrue(contains_index(query["query"]["explain_plan"], name))

    def test_expression_indexes_and_reranking_keep_original_ground_truth(self):
        for metric in ("l1", "l2", "ip", "cosine"):
            for representation in ("halfvec", "binary"):
                for method in ("hnsw", "ivfflat"):
                    if method == "ivfflat" and metric == "l1" and representation == "halfvec":
                        continue
                    with self.subTest(metric=metric, representation=representation, method=method):
                        manifest = self.fixture("vector", metric)
                        table = f"{self.schema}.t{self.counter}"
                        name = f"ann_{self.counter}"
                        config = ["--index-representation", representation]
                        self.cli(manifest, *config, f"--setup=table_name={table}", f"--load=table_name={table};client_num=1;thread_num=1",
                                 f"--index=table_name={table};index_name={name};index_type={method}" + (";lists=4" if method == "ivfflat" else ""))
                        query = self.cli(manifest, *config, f"--query=table_name={table};k1=1;k2=1;thread_num=1;rerank=true;candidate_k=256;hnsw.ef_search=256;ivfflat.probes=4;explain=true;require_index={name}", force_index=True)
                        self.assertEqual(query["query"]["recall"]["average"], 1)
                        self.assertEqual(query["query"]["config"]["evaluation_metric"], metric)
                        self.assertEqual(query["query"]["config"]["search_metric"], "hamming" if representation == "binary" else metric)
                        self.assertEqual(query["query"]["table"]["warnings"], [])
                        self.assertTrue(contains_index(query["query"]["explain_plan"], name))

    def test_rejects_incompatible_configs_and_sparse_nnz_before_create(self):
        manifest = self.fixture("bit", "jaccard")
        table = f"{self.schema}.bad"
        self.cli(manifest, f"--setup=table_name={table}", f"--index=table_name={table};index_type=ivfflat", success=False)
        self.assertIsNone(self.conn.execute("SELECT to_regclass(%s)", (table,)).fetchone()[0])
        base = csr_matrix(np.ones((2, 1001), dtype=np.float32))
        manifest = publish(self.root / "nnz", "nnz", base, base[:1], np.array([[0]]), "sparsevec", "ip", 1001, {})
        self.cli(manifest, f"--setup=table_name={table}", f"--load=table_name={table};client_num=1;thread_num=1")
        error = self.cli(manifest, f"--index=table_name={table};index_type=hnsw", success=False)
        self.assertIn("1000 nonzero", error)

    def test_rejects_mismatched_storage_and_reranking_options(self):
        manifest = self.fixture("vector", "l2")
        table = f"{self.schema}.typed"
        self.cli(manifest, "--storage-type", "halfvec", f"--setup=table_name={table}")
        error = self.cli(manifest, f"--load=table_name={table}", success=False)
        self.assertIn("expected vector", error)
        error = self.cli(manifest, "--storage-type", "halfvec", f"--query=table_name={table};rerank=true", success=False)
        self.assertIn("rerank requires", error)
        self.cli(manifest, "--dataset", "siftsmall", success=False)

    def test_required_index_fails_before_measurement_and_keeps_stdout_empty(self):
        manifest = self.fixture("vector", "l2")
        table = f"{self.schema}.exact_only"
        self.cli(manifest, f"--setup=table_name={table}", f"--load=table_name={table};thread_num=1;client_num=1")
        error = self.cli(manifest, f"--query=table_name={table};k1=1;k2=1;thread_num=1;require_index=missing_ann", success=False)
        self.assertIn("target index not used by sample plan", error)
        self.assertIn("Seq Scan", error)
        self.assertNotIn("qps:", error)

    def test_sparse_shards_reordered_queries_and_corrupt_input(self):
        manifest = self.fixture("sparsevec", "ip")
        description = json.loads(manifest.read_text())
        data = pq.read_table(manifest.parent / "train.parquet")
        pq.write_table(data.slice(0, 128), manifest.parent / "part0.parquet", compression="zstd")
        pq.write_table(data.slice(128), manifest.parent / "part1.parquet", compression="zstd")
        description["base_files"] = [{"path": "part0.parquet", "rows": 128}, {"path": "part1.parquet", "rows": 128}]
        manifest.write_text(json.dumps(description))
        for name in ["test.parquet", "neighbors.parquet"]:
            data = pq.read_table(manifest.parent / name)
            order = [4, 2, 0, 1, 3] if name == "test.parquet" else [1, 3, 2, 4, 0]
            pq.write_table(data.take(order), manifest.parent / name, compression="zstd")
        table = f"{self.schema}.shards"
        result = self.cli(manifest, f"--setup=table_name={table}", f"--load=table_name={table};thread_num=2;client_num=2",
                          f"--index=table_name={table};index_type=hnsw", f"--query=table_name={table};k1=1;k2=1;hnsw.ef_search=256;thread_num=1")
        self.assertEqual(result["load"]["rows"], 256)
        self.assertEqual(result["query"]["recall"]["average"], 1)
        (manifest.parent / "test.parquet").write_bytes(b"broken parquet")
        self.cli(manifest, f"--query=table_name={table};k1=1;k2=1;thread_num=1", success=False)

    def test_halfvec_overflow_and_iterative_settings(self):
        manifest = self.fixture("vector", "l2")
        table = f"{self.schema}.overflow"
        data = pq.read_table(manifest.parent / "train.parquet")
        emb = data["emb"].to_pylist()
        emb[0][0] = 1e8
        data = data.set_column(1, "emb", pa.array(emb, type=pa.list_(pa.float32())))
        pq.write_table(data, manifest.parent / "train.parquet", compression="zstd")
        self.cli(manifest, "--storage-type", "halfvec", f"--setup=table_name={table}")
        error = self.cli(manifest, "--storage-type", "halfvec", f"--load=table_name={table};client_num=1;thread_num=1", success=False)
        self.assertIn("out of range", error)
        manifest = self.fixture("vector", "cosine")
        table = f"{self.schema}.iterative"
        self.cli(manifest, f"--setup=table_name={table}", f"--load=table_name={table};client_num=1;thread_num=1", f"--index=table_name={table};index_type=hnsw")
        result = self.cli(manifest, f"--query=table_name={table};k1=1;k2=1;thread_num=1;hnsw.iterative_scan=strict_order;hnsw.max_scan_tuples=5000;hnsw.scan_mem_multiplier=2")
        self.assertEqual(result["query"]["effective_settings"]["hnsw.iterative_scan"], "strict_order")
        self.assertEqual(result["query"]["config"]["session_overrides"]["hnsw.scan_mem_multiplier"], "2")


if __name__ == "__main__":
    unittest.main()
