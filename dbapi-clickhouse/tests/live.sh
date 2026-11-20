#!/usr/bin/env bash
# Run the ClickHouse provider contract against a testcontainers-managed server.
#
# The integration test is `#[ignore]`d so a plain `cargo test` stays
# fixture-free. It starts and removes its own container; this script only checks
# Docker and selects the ignored test. The image is pinned by digest in
# tests/live.rs.
set -euo pipefail

cd "$(dirname "$0")/../.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required for the ClickHouse provider integration test" >&2
  exit 1
fi

list=$(cargo test -p neotales-dbapi-clickhouse --test live -- --ignored --list)
for test in clickhouse_native_provider_contract clickhouse_sql_page_source_contract verified_tls_rejects_wrong_ca_and_hostname; do
  if ! grep -Fxq "$test: test" <<< "$list"; then
    echo "selected live test missing: $test" >&2
    exit 1
  fi
done
cargo test -p neotales-dbapi-clickhouse --test live -- --ignored
