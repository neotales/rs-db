#!/bin/sh
# CI runs this from .github/workflows/dbapi.yml (satellite) and
# rs-dbapi.yml (monorepo). The server image is pinned by manifest digest so a
# moved tag cannot change what the lane runs.
set -eu
cd "$(dirname "$0")/../.."

# A random value is the only ownership token that is safe to use for cleanup.
# It is not printed and never appears in an error message.
token=$(od -An -N16 -tx1 /dev/urandom | tr -d '[:space:]')
if [ -z "$token" ]; then
    echo "failed to generate a unique container label" >&2
    exit 1
fi
label="neotales.lane=$token"

scratch_root=${TMPDIR:-/tmp/opencode}
mkdir -p "$scratch_root"
dir=$(mktemp -d "$scratch_root/plab-dbapi-tls.XXXXXX")
name1="plab-dbapi-tls-primary-$$"
name2="plab-dbapi-tls-ed25519-$$"
cidfile1="$dir/container-primary"
cidfile2="$dir/container-ed25519"
list_file="$dir/provider-tests.list"
container_id1=
container_id2=

remove_owned() {
    id=$1
    [ -n "$id" ] || return 0
    # Remove only the exact container this invocation recorded, after checking
    # its label. Never remove by name: a name can collide with another run.
    actual=$(docker inspect --format '{{ index .Config.Labels "neotales.lane" }}' "$id" 2>/dev/null || true)
    if [ "$actual" = "$token" ]; then
        if ! docker rm -f "$id" >/dev/null 2>&1; then
            echo "failed to clean owned disposable container $id" >&2
            return 1
        fi
    fi
    return 0
}

cleanup() {
    result=$?
    remove_owned "${container_id1:-}" || result=1
    remove_owned "${container_id2:-}" || result=1
    # docker run can create the container and still fail while writing --cidfile
    # (for example, ENOSPC). The unique label is the only safe way to find that
    # container without ever falling back to its name.
    if ! owned=$(docker ps --all --quiet --filter "label=$label" 2>/dev/null); then
        echo "failed to enumerate owned disposable containers" >&2
        result=1
        owned=""
    fi
    for id in $owned; do
        if [ "$id" = "${container_id1:-}" ] || [ "$id" = "${container_id2:-}" ]; then
            continue
        fi
        if ! docker rm -f "$id" >/dev/null 2>&1; then
            echo "failed to clean owned disposable container $id" >&2
            result=1
        fi
    done
    rm -f "$cidfile1" "$cidfile2"
    rm -rf "$dir"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

chmod 755 "$dir"
umask 077

# Primary fixture: RSA server certificate signed by a synthetic CA with SHA-256.
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$dir/ca.key" -out "$dir/ca.pem" \
    -days 1 -subj '/CN=plab synthetic test CA' >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$dir/server.key" -out "$dir/server.csr" \
    -subj '/CN=tls_fixture_cert_canary' >/dev/null 2>&1
printf 'subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n' > "$dir/ext.cnf"
openssl x509 -req -in "$dir/server.csr" -CA "$dir/ca.pem" -CAkey "$dir/ca.key" \
    -CAcreateserial -out "$dir/server.crt" -days 1 -extfile "$dir/ext.cnf" >/dev/null 2>&1
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$dir/other.key" -out "$dir/other.pem" \
    -days 1 -subj '/CN=other synthetic test CA' >/dev/null 2>&1
openssl x509 -in "$dir/ca.pem" -outform der -out "$dir/ca.der"
openssl x509 -in "$dir/other.pem" -outform der -out "$dir/other.der"

# Secondary fixture: a self-signed Ed25519 certificate. Ed25519 has no
# signature digest, so RFC 5929 tls-server-end-point cannot be computed from it.
openssl req -x509 -newkey ed25519 -nodes -keyout "$dir/ed25519.key" \
    -out "$dir/ed25519.pem" -days 1 \
    -subj '/CN=plab synthetic ed25519 fixture' \
    -addext 'subjectAltName=DNS:localhost' \
    -addext 'extendedKeyUsage=serverAuth' >/dev/null 2>&1
openssl x509 -in "$dir/ed25519.pem" -outform der -out "$dir/ed25519.der"

chmod 644 "$dir/server.crt" "$dir/ca.der" "$dir/other.der" "$dir/ed25519.der"

# A per-run synthetic SCRAM password for a throwaway loopback role. It is never
# printed and is passed to the tests through the environment only.
scram_user=plab_scram
scram_password=$(od -An -N24 -tx1 /dev/urandom | tr -d '[:space:]')
if [ -z "$scram_password" ]; then
    echo "failed to generate a synthetic SCRAM password" >&2
    exit 1
fi

# postgres:17-alpine pinned by manifest digest (PostgreSQL 17.12-alpine3.24).
image='postgres:17-alpine@sha256:d3244e8ce3f165ee3c93d9c4a4b1d0fe48b3a89e2287b717fdcbbde92d405065'

start_server() {
    cidfile=$1
    name=$2
    port=$3
    cert=$4
    key=$5
    docker run -d --rm --cidfile "$cidfile" --label "$label" --name "$name" \
        -p "127.0.0.1:$port:5432" \
        -e POSTGRES_HOST_AUTH_METHOD=trust -v "$dir:/fixture:ro" \
        --entrypoint sh "$image" -c "
          cp /fixture/$cert /tmp/server.crt
          cp /fixture/$key /tmp/server.key
          chown postgres:postgres /tmp/server.key /tmp/server.crt
          chmod 600 /tmp/server.key
          printf 'local all all trust\nhostnossl all all all reject\nhostssl all $scram_user all scram-sha-256\nhostssl all all all trust\n' > /tmp/test-hba
          chmod 644 /tmp/test-hba
          exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/tmp/server.crt \
              -c ssl_key_file=/tmp/server.key -c hba_file=/tmp/test-hba
        " >/dev/null
}

wait_ready() {
    cid=$1
    i=0
    until docker exec "$cid" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; do
        i=$((i + 1))
        if [ "$i" -ge 300 ]; then
            docker logs "$cid" >&2 || true
            echo "PostgreSQL did not become ready" >&2
            return 1
        fi
        sleep 0.2
    done
}

create_scram_role() {
    cid=$1
    # Keep the synthetic password out of error output. The SQL is read from
    # stdin, never argv, and is never echoed on success or failure.
    if ! printf "SET password_encryption='scram-sha-256';\nCREATE ROLE %s LOGIN PASSWORD '%s';\n" \
        "$scram_user" "$scram_password" \
        | docker exec -i "$cid" psql -U postgres -d postgres -v ON_ERROR_STOP=1 >/dev/null 2>&1; then
        echo "failed to create the synthetic SCRAM role" >&2
        return 1
    fi
}

start_server "$cidfile1" "$name1" 55433 server.crt server.key
if [ ! -s "$cidfile1" ]; then
    echo "docker run did not record the primary container ID" >&2
    exit 1
fi
container_id1=$(cat "$cidfile1")

start_server "$cidfile2" "$name2" 55434 ed25519.pem ed25519.key
if [ ! -s "$cidfile2" ]; then
    echo "docker run did not record the Ed25519 container ID" >&2
    exit 1
fi
container_id2=$(cat "$cidfile2")

wait_ready "$container_id1"
wait_ready "$container_id2"
create_scram_role "$container_id1"
create_scram_role "$container_id2"

DBAPI_TLS_TEST_CA_DER="$dir/ca.der"
DBAPI_TLS_TEST_OTHER_CA_DER="$dir/other.der"
DBAPI_TLS_TEST_ED25519_CA_DER="$dir/ed25519.der"
DBAPI_TLS_TEST_SCRAM_USER="$scram_user"
DBAPI_TLS_TEST_SCRAM_PASSWORD="$scram_password"
export DBAPI_TLS_TEST_CA_DER DBAPI_TLS_TEST_OTHER_CA_DER DBAPI_TLS_TEST_ED25519_CA_DER
export DBAPI_TLS_TEST_SCRAM_USER DBAPI_TLS_TEST_SCRAM_PASSWORD

cargo test -p neotales-dbapi-postgres-tls

# A removed or renamed ignored provider test must not turn into a green lane.
# Capture cargo's status separately: a POSIX pipeline would otherwise hide a
# failing Cargo command behind grep's success.
if ! cargo test -p neotales-dbapi-postgres --test tls_live -- --ignored --list >"$list_file"; then
    echo "cargo test --list failed for the provider TLS target" >&2
    exit 1
fi
if ! grep -c ': test$' "$list_file" >/dev/null; then
    echo "no ignored provider TLS test found; refusing to report a pass" >&2
    exit 1
fi

cargo test -p neotales-dbapi-postgres --test tls_live -- --ignored --test-threads=1
