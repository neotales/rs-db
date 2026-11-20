#!/usr/bin/env bash
# Run the forked TDS driver's live cancellation and pool-reuse contract.
#
# The integration test is `#[ignore]`d so a plain `cargo test` stays
# fixture-free. It starts and removes its own digest-pinned SQL Server
# container; this script only checks Docker, proves the exact ignored test was
# selected, and runs it.
set -euo pipefail

cd "$(dirname "$0")/../.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required for the forked SQL Server driver integration test" >&2
  exit 1
fi

scratch_root=${TMPDIR:-/tmp/opencode}
mkdir -p -- "$scratch_root"
list_file=$(mktemp "$scratch_root/dbapi-mssql-driver.XXXXXX")
trap 'rm -f -- "$list_file"' EXIT

if ! cargo test -p neotales-sqlx-mssql --test live -- --ignored --list >"$list_file"; then
  echo "cargo test --list failed for the driver live target" >&2
  exit 1
fi
if ! grep -qx 'boundaries_and_abandoned_stream_pool_reuse: test' "$list_file"; then
  echo "the exact ignored driver live test was not found; refusing to report a pass" >&2
  exit 1
fi

cargo test -p neotales-sqlx-mssql --test live -- --ignored --test-threads=1
