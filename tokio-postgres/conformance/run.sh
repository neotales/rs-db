#!/usr/bin/env bash
# Reproduce the unmodified tokio-postgres 0.7.18 upstream test suite against
# the quiet neotales-tokio-postgres fork in a disposable PostgreSQL 17 container.
#
# Requirements: Linux, bash, Docker, OpenSSL, coreutils sha256sum, and a free
# 127.0.0.1:5433. No network access is needed to obtain the test sources; they
# are vendored and hash-checked. Cargo may download ordinary crate dependencies
# unless the local cache already has them (CARGO_NET_OFFLINE=true is honoured).
set -euo pipefail

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

# Pinned to the same postgres:17 image by manifest digest. Tag drift cannot
# change what this lane runs.
image='postgres:17@sha256:846a7ddaa67a4b354ae1d21087b683901aa6113a4ed626d3787cfbf6c85e3e07'

# A random value is the only ownership token that is safe to use for cleanup.
# It is not printed and never appears in an error message.
token=$(od -An -N16 -tx1 /dev/urandom | tr -d '[:space:]')
if [[ -z "$token" ]]; then
    echo "failed to generate a unique container label" >&2
    exit 1
fi
label="neotales.lane=$token"

scratch_root=${TMPDIR:-/tmp}/opencode
mkdir -p -- "$scratch_root"
scratch=$(mktemp -d "$scratch_root/tokio-postgres-conformance.XXXXXX")
name="neotales-tokio-postgres-conformance-$$"
cidfile="$scratch/container"
list_file="$scratch/upstream-tests.list"
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
    rm -rf -- "$scratch"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

(
    cd -- "$here"
    sha256sum --check --quiet upstream.sha256
)
echo "upstream test sources match upstream.sha256"

# A missing upstream target would let fork_identity alone look like a pass.
# Capture cargo's status separately so a failing test-list command cannot be
# hidden by the grep in a pipeline.
if ! cargo test --locked --manifest-path "$here/Cargo.toml" --test upstream -- --list \
    >"$list_file"; then
    echo "cargo test --list failed for the vendored upstream target" >&2
    exit 1
fi
if ! grep -c ': test$' "$list_file" >/dev/null; then
    echo "the vendored upstream test target is missing or empty" >&2
    exit 1
fi

if (exec 3<>/dev/tcp/127.0.0.1/5433) 2>/dev/null; then
    echo "127.0.0.1:5433 is already in use; the upstream suite hardcodes that port" >&2
    exit 1
fi

umask 077
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$scratch/ca.key" -out "$scratch/ca.pem" \
    -days 1 -subj '/CN=neotales tokio-postgres conformance CA' >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$scratch/server.key" -out "$scratch/server.csr" \
    -subj '/CN=localhost' >/dev/null 2>&1
printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n' >"$scratch/server.ext"
openssl x509 -req -in "$scratch/server.csr" -CA "$scratch/ca.pem" -CAkey "$scratch/ca.key" \
    -CAcreateserial -out "$scratch/server.crt" -days 1 -extfile "$scratch/server.ext" >/dev/null 2>&1
chmod 644 "$scratch/ca.pem" "$scratch/server.crt"
mkdir "$scratch/socket"
chmod 777 "$scratch/socket"

docker run --detach --cidfile "$cidfile" --label "$label" --name "$name" \
    -e POSTGRES_DB=postgres -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p 127.0.0.1:5433:5433 \
    -v "$scratch:/certs:ro" \
    -v "$scratch/socket:/socket" \
    -v "$here/fixture:/fixture:ro" \
    -v "$here/fixture/roles.sql:/docker-entrypoint-initdb.d/10-roles.sql:ro" \
    --entrypoint sh "$image" -c '
        cp /certs/server.key /tmp/server.key
        cp /certs/server.crt /tmp/server.crt
        cp /fixture/pg_hba.conf /tmp/pg_hba.conf
        chown postgres:postgres /tmp/server.key /tmp/server.crt /tmp/pg_hba.conf
        chmod 600 /tmp/server.key
        exec docker-entrypoint.sh postgres -c port=5433 -c ssl=on \
            -c unix_socket_directories="/var/run/postgresql,/socket" \
            -c ssl_cert_file=/tmp/server.crt -c ssl_key_file=/tmp/server.key \
            -c hba_file=/tmp/pg_hba.conf
    ' >/dev/null

if [[ ! -s "$cidfile" ]]; then
    echo "docker run did not record a container ID" >&2
    exit 1
fi
container_id=$(<"$cidfile")

ready=0
for _ in $(seq 1 120); do
    if docker exec "$container_id" psql -h 127.0.0.1 -p 5433 -U postgres -d postgres \
        -tAc 'SELECT 1' >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 0.25
done
if [[ "$ready" != 1 ]]; then
    echo "disposable PostgreSQL 17 did not become ready" >&2
    docker logs "$container_id" >&2 || true
    exit 1
fi

# Only the classification leaves the server, never the verifier.
verifier_class=$(docker exec "$container_id" psql -h 127.0.0.1 -p 5433 -U postgres -d postgres -tAc \
    "SELECT CASE WHEN left(rolpassword, 3) = 'md5' THEN 'md5' ELSE 'other' END
     FROM pg_authid WHERE rolname = 'md5_user'")
if [[ "$verifier_class" != "md5" ]]; then
    echo "md5_user does not have an MD5 verifier; refusing to report MD5 coverage" >&2
    exit 1
fi
echo "PostgreSQL 17 fixture ready on 127.0.0.1:5433 ($image)"
echo "md5_user verifier classification: md5"
CONFORMANCE_SOCKET_DIR="$scratch/socket" cargo test --locked --manifest-path "$here/Cargo.toml" "$@"
