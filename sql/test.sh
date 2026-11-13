#!/usr/bin/env bash
# The protocol's tests: starts the pinned postgres:17 of the specification's runner in Docker, then runs the harness
# against it. Arguments go to `cargo test`; CRATE picks the crate whose tests run (the harness by default).
#
#   bash sql/test.sh                 every test
#   bash sql/test.sh --test r0_skeleton
#   CRATE=metalake bash sql/test.sh
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
image=${IMAGE:-postgres:17@sha256:3b0cda93be7c8dba2ed484d19393d93845dc3dafb88973e248ba8e6d99d784e1}
name=metalake-protocol-pg; port=${PORT:-55433}
docker rm -f "$name" >/dev/null 2>&1 || true
# On Windows, WSL relays every port Docker publishes on 127.0.0.1 and ::1, and a connection to localhost can reach
# the relay instead of Docker and hang: connect through the machine's own address, where only Docker listens.
host=localhost
if command -v powershell >/dev/null 2>&1; then
  host=$(powershell -NoProfile -Command "(Get-NetIPAddress -AddressFamily IPv4 |
    Where-Object { \$_.InterfaceAlias -notmatch 'Loopback|vEthernet|WSL' -and \$_.IPAddress -notlike '169.*' } |
    Select-Object -First 1).IPAddress" | tr -d '\r')
  host=${host:-localhost}
fi
docker run -d --name "$name" -p "$port:5432" -e POSTGRES_HOST_AUTH_METHOD=trust "$image" \
  -c track_commit_timestamp=on -c fsync=off ${PGARGS:-} >/dev/null
trap 'docker rm -f "$name" >/dev/null' EXIT
until docker exec "$name" pg_isready -q -U postgres 2>/dev/null; do sleep 1; done; sleep 2
if [ -n "${TLS:-}" ]; then
  # TLS=1 also serves TLS, by a server certificate for metalake-test.invalid that a CA of this run signs; another CA
  # signs nothing. The role tls_only may connect only over TLS; by_password only by its password, which has none until
  # a test sets one. METALAKE_TEST_TLS names the directory of ca.pem and other-ca.pem.
  # On Windows the paths are written as the system's, so that no argument is rewritten as one, `/CN=…` included.
  export MSYS_NO_PATHCONV=1
  tls=$(mktemp -d); if command -v cygpath >/dev/null 2>&1; then tls=$(cygpath -m "$tls"); fi; trap 'docker rm -f "$name" >/dev/null; rm -rf "$tls"' EXIT
  for ca in ca other-ca; do
    openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=metalake $ca" -keyout "$tls/$ca.key" \
      -out "$tls/$ca.pem" -addext "basicConstraints=critical,CA:TRUE" \
      -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
  done
  openssl req -newkey rsa:2048 -nodes -subj "/CN=metalake-test.invalid" -keyout "$tls/server.key" \
    -out "$tls/server.csr" 2>/dev/null
  printf 'subjectAltName=DNS:metalake-test.invalid\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n' \
    > "$tls/server.ext"
  openssl x509 -req -in "$tls/server.csr" -CA "$tls/ca.pem" -CAkey "$tls/ca.key" -CAcreateserial -days 2 \
    -extfile "$tls/server.ext" -out "$tls/server.crt" 2>/dev/null
  data=/var/lib/postgresql/data
  docker cp "$tls/server.crt" "$name:$data/server.crt" >/dev/null
  docker cp "$tls/server.key" "$name:$data/server.key" >/dev/null
  docker exec "$name" sh -c "chown postgres:postgres $data/server.* && chmod 600 $data/server.key &&
    sed -i -e '1i hostssl all tls_only all trust' -e '1i host all tls_only all reject' \
      -e '1i host all by_password all scram-sha-256' $data/pg_hba.conf"
  docker exec "$name" psql -q -U postgres -c "alter system set ssl = on" -c "create role tls_only login" \
    -c "create role by_password login" -c "select pg_reload_conf()" >/dev/null
  export METALAKE_TEST_TLS="$tls"
fi
export DATABASE_URL="host=$host port=$port user=postgres dbname=postgres"
cd "$here/.." && cargo test -p "${CRATE:-metalake-protocol-tests}" "$@"
