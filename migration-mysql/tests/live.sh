#!/usr/bin/env bash
# Run the MySQL migration contract against a testcontainers-managed server.
#
# The integration test is `#[ignore]`d so a plain `cargo test` stays
# fixture-free. It starts and removes its own container; this script only
# checks Docker and selects the ignored test. The image is pinned by digest in
# tests/live.rs.
set -euo pipefail

cd "$(dirname "$0")/../.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required for the MySQL migration integration test" >&2
  exit 1
fi

cargo test -p neotales-migration-mysql --all-targets -- --ignored
