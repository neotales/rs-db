#!/usr/bin/env bash
# CI runs this from .github/workflows/migrations.yml (satellite) and
# rs-migrations.yml (monorepo). The server image is pinned by manifest digest so a
# moved tag cannot change what the lane runs.
set -euo pipefail

cd "$(dirname "$0")/../.."

# A random value is the only ownership token that is safe to use for cleanup.
# It is not printed and never appears in an error message.
token=$(od -An -N16 -tx1 /dev/urandom | tr -d '[:space:]')
if [[ -z "$token" ]]; then
  echo "failed to generate a unique container label" >&2
  exit 1
fi
label="neotales.lane=$token"

scratch_root=${TMPDIR:-/tmp/opencode}
mkdir -p -- "$scratch_root"
scratch=$(mktemp -d "$scratch_root/migration-postgres.XXXXXX")
name="neotales-migration-postgres-${scratch##*.}"
cidfile="$scratch/container"
list_file="$scratch/live-tests.list"
container_id=

cleanup() {
  local result=$?
  local id actual
  # Remove only the exact container this invocation recorded, after checking
  # its label. Never remove by name: a name can collide with another run.
  if [[ -n "${container_id:-}" ]]; then
    actual=$(docker inspect --format '{{ index .Config.Labels "neotales.lane" }}' "$container_id" 2>/dev/null || true)
    if [[ "$actual" == "$token" ]]; then
      if ! docker rm --force --volumes "$container_id" >/dev/null 2>&1; then
        echo "failed to clean owned disposable container $container_id" >&2
        result=1
      fi
    fi
  fi
  # docker run can create the container and still fail while writing --cidfile
  # (for example, ENOSPC). The unique label is the only safe way to find that
  # container without ever falling back to its name.
  if ! owned=$(docker ps --all --quiet --filter "label=$label" 2>/dev/null); then
    echo "failed to enumerate owned disposable containers" >&2
    result=1
    owned=""
  fi
  while IFS= read -r id; do
    [[ -n "$id" ]] || continue
    if [[ -n "${container_id:-}" && "$id" == "$container_id" ]]; then
      continue
    fi
    if ! docker rm --force --volumes "$id" >/dev/null 2>&1; then
      echo "failed to clean owned disposable container $id" >&2
      result=1
    fi
  done <<< "$owned"
  rm -f -- "$cidfile" "$list_file"
  rmdir -- "$scratch" 2>/dev/null || true
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

# postgres:17 pinned by manifest digest (PostgreSQL 17.12 at the time of writing).
image='postgres:17@sha256:846a7ddaa67a4b354ae1d21087b683901aa6113a4ed626d3787cfbf6c85e3e07'
docker run --detach --cidfile "$cidfile" --label "$label" --name "$name" \
  -e POSTGRES_USER=migrator_test -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_DB=migrator_test -p 127.0.0.1:55435:5432 "$image" >/dev/null

if [[ ! -s "$cidfile" ]]; then
  echo "docker run did not record a container ID" >&2
  exit 1
fi
container_id=$(<"$cidfile")

ready=0
for _ in $(seq 1 60); do
  if docker exec "$container_id" pg_isready -h 127.0.0.1 -U migrator_test -d migrator_test >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  docker logs "$container_id" >&2 || true
  echo "disposable PostgreSQL 17 did not become ready" >&2
  exit 1
fi

# A removed or renamed ignored test must not turn into a green lane. Capture
# cargo's status separately so a failing test-list command cannot be hidden by
# the grep in a pipeline.
if ! cargo test -p neotales-migration-postgres --test live -- --ignored --list >"$list_file"; then
  echo "cargo test --list failed for the migration live target" >&2
  exit 1
fi
if ! grep -c ': test$' "$list_file" >/dev/null; then
  echo "no ignored migration live tests found; refusing to report a pass" >&2
  exit 1
fi

NEOTALES_MIGRATION_POSTGRES_LIVE=disposable-17 \
  cargo test -p neotales-migration-postgres --test live -- --ignored --test-threads=1
