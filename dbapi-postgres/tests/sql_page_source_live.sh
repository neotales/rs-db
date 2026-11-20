#!/usr/bin/env bash
# Run the PostgreSQL shared SQL page-source contract against a disposable container.
#
# The test is `#[ignore]`d so a plain `cargo test` stays fixture-free. It starts and
# removes its own digest-pinned container, publishes no host port, and refuses a
# successful lane if the exact ignored test is missing.
set -euo pipefail

cd "$(dirname "$0")/../.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required for the PostgreSQL SQL page-source test" >&2
  exit 1
fi

scratch_root=${TMPDIR:-/tmp/opencode}
mkdir -p -- "$scratch_root"
list_file=$(mktemp "$scratch_root/dbapi-postgres-sql-page-source.XXXXXX")
trap 'rm -f -- "$list_file"' EXIT

if ! cargo test -p neotales-dbapi-postgres --test sql_page_source_live -- --ignored --list >"$list_file"; then
  echo "cargo test --list failed for the PostgreSQL page-source target" >&2
  exit 1
fi
if ! grep -Fxq 'postgres_sql_page_source_contract: test' "$list_file"; then
  echo "the exact ignored PostgreSQL page-source test was not found; refusing to report a pass" >&2
  exit 1
fi

cargo test -p neotales-dbapi-postgres --test sql_page_source_live -- --ignored --test-threads=1
