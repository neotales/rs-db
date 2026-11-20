#!/usr/bin/env bash
# Run the remote libSQL/Turso provider contract against a disposable Docker sqld.
#
# The integration test is `#[ignore]`d so a plain `cargo test` stays fixture-free.
# The container is created and removed by the test, publishes no host port, and is
# pinned by index digest in tests/sqld.rs. Cleartext is used only through a
# loopback proxy, matching the provider's documented local-only exception.
set -euo pipefail

cd "$(dirname "$0")/../.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required for the libSQL provider integration test" >&2
  exit 1
fi

image="ghcr.io/tursodatabase/libsql-server@sha256:6dd3eb276d9d3604e4a48ac4a999a2e267814732d57d7e94c04ba71482333a67"
if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "The digest-pinned libSQL server image is not present locally: $image" >&2
  exit 1
fi

scratch_root=${TMPDIR:-/tmp/opencode}
mkdir -p -- "$scratch_root"
list_file=$(mktemp "$scratch_root/sqld-sql-page-source.XXXXXX")
trap 'rm -f -- "$list_file"' EXIT
cargo test -p neotales-dbapi-libsql --test sqld -- --ignored --list >"$list_file"
if ! grep -Fxq 'sqld_sql_page_source_contract: test' "$list_file"; then
  echo 'the exact ignored SQL page-source test was not found; refusing to report a pass' >&2
  exit 1
fi

cargo test -p neotales-dbapi-libsql --all-targets -- --ignored
