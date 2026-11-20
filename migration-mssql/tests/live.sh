#!/usr/bin/env bash
# Run the SQL Server migration contract against a testcontainers-managed server.
#
# The integration test is `#[ignore]`d so a plain `cargo test` stays
# fixture-free. It starts and removes its own digest-pinned container; this
# script only checks Docker, proves the exact ignored test exists, and selects it.
set -euo pipefail

cd "$(dirname "$0")/../.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required for the SQL Server migration integration test" >&2
  exit 1
fi

scratch_root=${TMPDIR:-/tmp/opencode}
mkdir -p -- "$scratch_root"
list_file=$(mktemp "$scratch_root/migration-mssql.XXXXXX")
trap 'rm -f -- "$list_file"' EXIT

if ! cargo test -p neotales-migration-mssql --test live -- --ignored --list >"$list_file"; then
  echo "cargo test --list failed for the migration live target" >&2
  exit 1
fi
if ! grep -qx 'mssql_migration_contract: test' "$list_file"; then
  echo "the exact ignored migration live test was not found; refusing to report a pass" >&2
  exit 1
fi

cargo test -p neotales-migration-mssql --all-targets -- --ignored --test-threads=1
