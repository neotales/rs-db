#!/usr/bin/env bash
# A downstream crate must not see test-only insecure constructors under any
# feature/profile combination, including release with debug assertions enabled.
set -euo pipefail

crate_root=$(cd "$(dirname "$0")/.." && pwd)
workspace_root=$(cd "$crate_root/.." && pwd)
scratch_root=${TMPDIR:-/tmp/opencode}
mkdir -p -- "$scratch_root"
scratch=$(mktemp -d "$scratch_root/mssql-security.XXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
mkdir -p "$scratch/src"
cat >"$scratch/Cargo.toml" <<EOF
[package]
name = "mssql-security-boundary"
version = "0.0.0"
edition = "2024"

[features]
fixture-opt-in = []

[dependencies.neotales-dbapi-mssql-direct]
path = "$crate_root"
EOF
cat >"$scratch/src/main.rs" <<'EOF'
use neotales_dbapi_mssql_direct::{ExclusivePool, Probe};

fn main() {
    let _ = Probe::connect_insecure_test;
    let _ = ExclusivePool::insecure_test;
}
EOF

export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-"$workspace_root/target"}
check_rejected() {
  local label=$1
  shift
  local output="$scratch/$label.log"
  if "$@" >"$output" 2>&1; then
    echo "downstream unexpectedly compiled insecure APIs ($label)" >&2
    exit 1
  fi
  if ! grep -Fq "connect_insecure_test" "$output" ||
    ! grep -Fq "insecure_test" "$output"; then
    cat "$output" >&2
    echo "downstream failed for an unexpected reason ($label)" >&2
    exit 1
  fi
}

check_rejected debug-all-features cargo check \
  --manifest-path "$scratch/Cargo.toml" --all-features
check_rejected release-debug-assertions env \
  RUSTFLAGS="-C debug-assertions=yes" cargo check --release \
  --manifest-path "$scratch/Cargo.toml" --all-features
