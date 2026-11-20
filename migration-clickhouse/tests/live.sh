#!/usr/bin/env bash
# The default lane verifies fail-closed behavior. The experimental lane runs
# the same recovery contract against an enabled ordinary dependency build.
# The disposable image is pinned by digest in tests/support/live.rs.
set -euo pipefail

cd "$(dirname "$0")/../.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is required for the ClickHouse migration integration test" >&2
  exit 1
fi

case "${1:-}" in
  "")
    args=(-p neotales-migration-clickhouse --test live)
    selected=the_backend_fails_closed_on_the_fetch_only_provider
    ;;
  --test-harness)
    args=(-p neotales-migration-clickhouse --features experimental-native-migrations --lib)
    selected=clickhouse_migration_contract
    ;;
  --experimental|--all-features)
    args=(-p neotales-migration-clickhouse --features experimental-native-migrations --test live)
    selected=experimental_dependency_recovery_contract
    ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac

list=$(cargo test "${args[@]}" -- --ignored --list)
if ! grep -Fxq "$selected: test" <<< "$list"; then
  echo "selected live test missing: $selected" >&2
  exit 1
fi
cargo test "${args[@]}" -- --ignored --exact "$selected" --nocapture
