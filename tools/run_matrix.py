#!/usr/bin/env python3
"""Run reproducible index sweeps in an isolated schema and save JSON results.

Uses libpq PGHOST/PGPORT/PGUSER/PGPASSWORD plus the required --database. Only
objects in a fresh generated schema are created/dropped. No global SET changes.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import psycopg
from psycopg import sql


class RunFailure(RuntimeError):
    def __init__(self, label, returncode, diagnostics):
        super().__init__(f"{label} failed (exit {returncode}); see {label}.log")
        self.planner_rejected = "target index not used by sample plan:" in diagnostics
        self.log = label + ".log"


def profiles(source, metric):
    types = [(source, "native")]
    if source == "vector":
        types += [("halfvec", "native"), ("vector", "halfvec"), ("vector", "binary")]
    for storage, representation in types:
        for method in ("hnsw", "ivfflat"):
            if method == "ivfflat" and (storage == "sparsevec" or (representation != "binary" and metric in ("l1", "jaccard"))):
                continue
            yield storage, representation, method


def contains_index(plan, name):
    if isinstance(plan, dict):
        return plan.get("Index Name") == name or any(contains_index(v, name) for v in plan.values())
    if isinstance(plan, list):
        return any(contains_index(v, name) for v in plan)
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-config", type=Path, required=True)
    parser.add_argument("--binary", type=Path, default=Path("build/pgvectorbench"))
    parser.add_argument("--database", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--maintenance-work-mem", help="optional build-session memory override, e.g. 512MB")
    parser.add_argument("--method", choices=["hnsw", "ivfflat"])
    parser.add_argument("--storage-type", choices=["vector", "halfvec", "bit", "sparsevec"])
    parser.add_argument("--representation", choices=["native", "halfvec", "binary"])
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("threads must be positive")
    ds = json.loads(args.dataset_config.read_text())
    if ds["ground_truth"]["neighbors"] < 10:
        parser.error("matrix requires at least 10 ground truth neighbors")
    args.output.mkdir(parents=True, exist_ok=False)
    schema = "pgvb_" + uuid.uuid4().hex[:12]
    summary = []
    command = [str(args.binary.resolve()), "--dataset-config", str(args.dataset_config.resolve()),
               "--dbname", args.database, "--json"]
    def invoke(extra, label):
        run = subprocess.run(command + extra, text=True, capture_output=True)
        (args.output / (label + ".log")).write_text(run.stderr)
        if run.returncode:
            raise RunFailure(label, run.returncode, run.stderr)
        result = json.loads(run.stdout)
        (args.output / (label + ".json")).write_text(json.dumps(result, indent=2) + "\n")
        return result
    with psycopg.connect(dbname=args.database, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            for number, (storage, representation, method) in enumerate(profiles(ds["vector_type"], ds["metric"])):
                if (args.method and args.method != method) or (args.storage_type and args.storage_type != storage) or (args.representation and args.representation != representation):
                    continue
                label = f"{storage}_{representation}_{method}"
                table = f"{schema}.case_{number}"
                qualified = sql.Identifier(schema, f"case_{number}")
                index = f"case_{number}_ann"
                config = ["--storage-type", storage, "--index-representation", representation]
                print(f"running {label}", flush=True)
                try:
                    invoke(config + [f"--setup=table_name={table}", f"--load=table_name={table};client_num=2;thread_num={min(2, len(ds['base_files']))}"], label + "_load")
                    conn.execute(sql.SQL("ANALYZE {}").format(qualified))
                    build_options = "m=16;ef_construction=64" if method == "hnsw" else "lists=100"
                    if args.maintenance_work_mem:
                        build_options += ";maintenance_work_mem=" + args.maintenance_work_mem
                    invoke(config + [f"--index=table_name={table};index_name={index};index_type={method};{build_options}"], label + "_index")
                    setting = "hnsw.ef_search" if method == "hnsw" else "ivfflat.probes"
                    values = (40, 100, 200) if method == "hnsw" else (1, 10, 50)
                    for value in values:
                        candidates = (None, 100, 200) if storage == "vector" and representation != "native" else (None,)
                        for candidate in candidates:
                            run_label = f"{label}_{value}_" + ("direct" if candidate is None else f"rerank_{candidate}")
                            query = f"table_name={table};k1=10;k2=10;thread_num={args.threads};{setting}={value};percentages=50,95,99;require_index={index}"
                            if candidate:
                                query += f";rerank=true;candidate_k={candidate}"
                            try:
                                invoke(config + ["--query=" + query + ";loop=1"], run_label + "_warmup")
                                measured = invoke(config + ["--query=" + query + ";loop=3;explain=true"], run_label)
                                valid = contains_index(measured["query"]["explain_plan"], index)
                                summary.append({"profile": run_label, "status": "measured" if valid else "planner_did_not_use_target_index",
                                                "plan_scope": "first query, same session settings, outside measurement",
                                                "result": run_label + ".json"})
                            except RunFailure as error:
                                summary.append({"profile": run_label, "status": "planner_did_not_use_target_index" if error.planner_rejected else "failed",
                                                "error": str(error), "log": error.log})
                except Exception as error:
                    summary.append({"profile": label, "status": "failed", "error": str(error)})
                finally:
                    conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(qualified))
        finally:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    if not summary or any(r["status"] != "measured" for r in summary):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
