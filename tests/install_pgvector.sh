#!/usr/bin/env bash
set -euo pipefail

# pgvector v0.8.1, pinned for the database integration tests.
pgvector_test_source="$(mktemp -d)"
trap 'rm -rf "$pgvector_test_source"' EXIT
git -C "$pgvector_test_source" init --quiet
git -C "$pgvector_test_source" fetch --quiet --depth=1 \
  https://github.com/pgvector/pgvector.git 778dacf20c07caf904557a88705142631818d8cb
git -C "$pgvector_test_source" checkout --quiet FETCH_HEAD
pgvector_test_config="$(command -v pg_config)"
# These correctness tests do not need optional LLVM bitcode. The PGXS package
# can reference a clang version that is not installed on the runner.
make -C "$pgvector_test_source" -j2 PG_CONFIG="$pgvector_test_config" with_llvm=no
sudo make -C "$pgvector_test_source" PG_CONFIG="$pgvector_test_config" with_llvm=no install
psql -X -v ON_ERROR_STOP=1 -d postgres -c 'CREATE EXTENSION IF NOT EXISTS vector'
