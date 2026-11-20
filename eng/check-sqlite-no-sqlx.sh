#!/usr/bin/env bash
# Check that direct SQLite runtime packages do not pull SQLx into normal deps.
# Consumer test targets have separate SQLx dev-dependencies for compatibility
# and database inspection; this script deliberately checks only the normal graph.
set -euo pipefail

cd "$(dirname "$0")/.."

packages=(
  neotales-dbapi-sqlite-direct
  neotales-migration-sqlite
)

for package in "${packages[@]}"; do
  tree="$(cargo tree -p "$package" --no-default-features -e normal)"
  if grep -Eiq '(^|[[:space:]])sqlx([[:space:]]|-[[:alnum:]_.-]+[[:space:]])' <<<"$tree"; then
    echo "SQLx unexpectedly appears in the normal dependency graph for $package" >&2
    exit 1
  fi
  echo "$package normal dependency graph: SQLx absent"
done

# Compile the consumer libraries with compatibility features disabled. This
# complements the separate --all-targets test lane without activating dev deps.
cargo check \
  -p neotales-dbapi-sqlite-direct \
  -p neotales-migration-sqlite \
  --no-default-features \
  --lib
