#!/usr/bin/env bash
# Run the MySQL provider contract against a testcontainers-managed server.
#
# The integration tests are `#[ignore]`d so a plain `cargo test` stays
# fixture-free. They start and remove their own containers; this script only
# checks Docker and selects the ignored tests. The image is pinned by digest in
# tests/mysql.rs. The verified-TLS test also needs `openssl` to generate a
# disposable CA and server certificate.
set -euo pipefail

cd "$(dirname "$0")/../.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required for the MySQL provider integration test" >&2
  exit 1
fi

# Each ignored test owns a MySQL server. Limit this lane to one at a time rather
# than starting six cold servers simultaneously; individual tests stay isolated.
scratch_root=${TMPDIR:-/tmp/opencode}
mkdir -p -- "$scratch_root"
list_file=$(mktemp "$scratch_root/mysql-sql-page-source.XXXXXX")
trap 'rm -f -- "$list_file"' EXIT
cargo test -p neotales-dbapi-mysql --test mysql -- --ignored --list >"$list_file"
if ! grep -Fxq 'mysql_sql_page_source_contract: test' "$list_file"; then
  echo 'the exact ignored SQL page-source test was not found; refusing to report a pass' >&2
  exit 1
fi

cargo test -p neotales-dbapi-mysql --all-targets -- --ignored --test-threads=1
