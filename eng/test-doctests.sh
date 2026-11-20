#!/usr/bin/env bash
# Run the doctests for one pipeline group.
#
# CI runs this from .github/workflows/ci.yml. `cargo test --all-targets` does
# not run doctests, so that lane needs this step explicitly or a broken
# documentation example is invisible.
#
# Usage: eng/test-doctests.sh [dbapi|migration|audit|all]
#
# `all` is the default so a local caller keeps the previous whole-data
# behaviour. The pipelines pass their own group, so an audit package cannot
# turn a dbapi gate green or the other way round.
#
# This lane is fixture-free on purpose. The connector examples are `no_run`;
# the examples that execute are in-memory. No live-server opt-in, DSN or
# credential is read here. A doctest that needs a database is a finding for a
# live lane, not a reason to make the check job depend on a fixture.
#
# The --list preflight refuses an empty doctest set, the same rule the live
# scripts apply to their ignored tests: deleting every example must not turn
# into a green step.
set -euo pipefail

cd "$(dirname "$0")/.."

group=${1:-all}
case "$group" in
  dbapi)
    packages=(
      neotales-dbapi
      neotales-dbapi-d1
      neotales-dbapi-sqlite
      neotales-dbapi-libsql
      neotales-dbapi-mysql
      neotales-dbapi-mariadb
      neotales-dbapi-mssql
      neotales-dbapi-clickhouse
      neotales-dbapi-sqlite-direct
      neotales-dbapi-postgres
      neotales-dbapi-postgres-tls
      neotales-dbapi-mssql-direct
      neotales-tokio-postgres
    )
    ;;
  migration)
    packages=(
      neotales-migration
      neotales-migration-sqlite
      neotales-migration-libsql
      neotales-migration-postgres
      neotales-migration-mysql
      neotales-migration-mariadb
      neotales-migration-mssql
      neotales-migration-clickhouse
    )
    ;;
  all)
    packages=(
      neotales-dbapi
      neotales-dbapi-d1
      neotales-dbapi-sqlite
      neotales-dbapi-libsql
      neotales-dbapi-mysql
      neotales-dbapi-mariadb
      neotales-dbapi-mssql
      neotales-dbapi-clickhouse
      neotales-dbapi-sqlite-direct
      neotales-dbapi-postgres
      neotales-dbapi-postgres-tls
      neotales-dbapi-mssql-direct
      neotales-tokio-postgres
      neotales-migration
      neotales-migration-sqlite
      neotales-migration-libsql
      neotales-migration-postgres
      neotales-migration-mysql
      neotales-migration-mariadb
      neotales-migration-mssql
      neotales-migration-clickhouse
    )
    ;;
  *)
    echo "usage: eng/test-doctests.sh [dbapi|migration|all]" >&2
    exit 2
    ;;
esac

# A local run may inherit a live-lane opt-in from the caller. CI sets neither,
# and a doctest must never become a live test because of the environment.
unset NEOTALES_MIGRATION_POSTGRES_LIVE

# RUSTDOCFLAGS matches the Documentation step and governs rustdoc lints where
# they run. It does not turn compiler lints inside a doctest snippet into
# errors: rustdoc compiles those snippets separately. A snippet that fails to
# compile or a test that fails still fails this lane.
RUSTDOCFLAGS='-D warnings'
export RUSTDOCFLAGS

args=()
for package in "${packages[@]}"; do
  args+=(-p "$package")
done

if ! list=$(cargo test --doc "${args[@]}" -- --list); then
  echo "cargo test --doc --list failed; refusing to report a pass" >&2
  exit 1
fi
if ! grep -q ': test$' <<< "$list"; then
  echo "no doctests found for the ${group} pipeline packages; refusing to report a pass" >&2
  exit 1
fi

log_file=$(mktemp)
trap 'rm -f -- "$log_file"' EXIT

cargo test --doc "${args[@]}" 2>&1 | tee "$log_file"

# An `ignore` example is a silent skip. An example that only needs to compile
# uses `no_run`; an example that needs a server belongs in a live job.
if grep -Eq '[1-9][0-9]* ignored' "$log_file"; then
  echo "an ignored doctest was skipped; refusing to report a pass" >&2
  exit 1
fi
