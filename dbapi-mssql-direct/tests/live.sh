#!/usr/bin/env bash
# Runs the one pinned, disposable SQL Server fixture test and refuses a
# successful lane if the exact ignored test is missing.
set -euo pipefail

cd "$(dirname "$0")/../.."
scratch_root=${TMPDIR:-/tmp/opencode}
mkdir -p -- "$scratch_root"
list_file=$(mktemp "$scratch_root/mssql-direct-tests.XXXXXX")
trap 'rm -f -- "$list_file"' EXIT

if ! cargo test -p neotales-dbapi-mssql-direct --lib \
  -- --ignored --list >"$list_file"; then
  echo "could not list the SQL Server live tests" >&2
  exit 1
fi
if ! grep -Fxq 'live::direct_live: test' "$list_file"; then
  echo "expected ignored test live::direct_live is missing" >&2
  exit 1
fi
if ! grep -Fxq 'live::direct_tls_validation: test' "$list_file"; then
  echo "expected ignored test live::direct_tls_validation is missing" >&2
  exit 1
fi
if ! grep -Fxq 'live::direct_sql_page_source_live: test' "$list_file"; then
  echo "expected ignored test live::direct_sql_page_source_live is missing" >&2
  exit 1
fi

cargo test -p neotales-dbapi-mssql-direct --lib live::direct_live \
  -- --ignored --exact --nocapture
cargo test -p neotales-dbapi-mssql-direct --lib live::direct_tls_validation \
  -- --ignored --exact --nocapture
cargo test -p neotales-dbapi-mssql-direct --lib live::direct_sql_page_source_live \
  -- --ignored --exact --nocapture
