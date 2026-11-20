#!/usr/bin/env bash
# Run the SQL Server provider contract against a testcontainers-managed server.
#
# The integration test is `#[ignore]`d so a plain `cargo test` stays
# fixture-free. It starts and removes its own container; this script only checks
# Docker, proves the exact ignored test was selected, and runs it. The image is
# pinned by digest in tests/live.rs.
set -euo pipefail

cd "$(dirname "$0")/../.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required for the SQL Server provider integration test" >&2
  exit 1
fi

scratch_root=${TMPDIR:-/tmp/opencode}
mkdir -p -- "$scratch_root"
list_file=$(mktemp "$scratch_root/dbapi-mssql.XXXXXX")
trap 'rm -f -- "$list_file"' EXIT

if ! cargo test -p neotales-dbapi-mssql --test live -- --ignored --list >"$list_file"; then
  echo "cargo test --list failed for the provider live target" >&2
  exit 1
fi
if ! grep -qx 'mssql_provider_contract: test' "$list_file"; then
  echo "the exact ignored provider live test was not found; refusing to report a pass" >&2
  exit 1
fi
if ! grep -qx 'mssql_sql_page_source_contract: test' "$list_file"; then
  echo "the exact ignored SQL page-source test was not found; refusing to report a pass" >&2
  exit 1
fi

cargo test -p neotales-dbapi-mssql --all-targets -- --ignored --test-threads=1
