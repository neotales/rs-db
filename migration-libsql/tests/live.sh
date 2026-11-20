#!/usr/bin/env bash
# Run the libSQL migration contract against a disposable Docker sqld.
#
# The integration test is `#[ignore]`d so a plain `cargo test` stays fixture-free.
# The container is created and removed by the test, publishes no host port, and is
# pinned by index digest in tests/live.rs. Cleartext is used only through a
# loopback proxy, matching the provider's documented local-only exception.
set -euo pipefail

cd "$(dirname "$0")/../.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required for the libSQL migration integration test" >&2
  exit 1
fi

image="ghcr.io/tursodatabase/libsql-server@sha256:6dd3eb276d9d3604e4a48ac4a999a2e267814732d57d7e94c04ba71482333a67"
if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "The digest-pinned libSQL server image is not present locally: $image" >&2
  exit 1
fi

list=$(cargo test -p neotales-migration-libsql --test live -- --list)
for test in sqld_migration_contract lost_commit_response_requires_head_reconciliation cancelled_commit_requires_full_history_reconciliation existing_ledger_triggers_are_not_an_ownership_check; do
  if ! grep -Fxq "$test: test" <<<"$list"; then
    echo "Missing required libSQL migration live test: $test" >&2
    exit 1
  fi
done

cargo test -p neotales-migration-libsql --all-targets -- --ignored
