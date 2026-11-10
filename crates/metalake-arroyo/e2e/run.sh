#!/usr/bin/env bash
# The end-to-end test of MetaLake in Arroyo: Arroyo built from our fork, MetaLake's command line from this repository,
# a Postgres for the catalog and a volume for the lake. A pipeline follows a table of orders and keeps totals by
# customer in another; after each change to the orders, the totals must come to what the orders give, read by
# DataFusion through MetaLake. Inserts, deletes, a customer gone, and a restart from the pipeline's checkpoint.
# The pipeline is standard SQL over the lake's names, with no table declared: Arroyo finds them in the catalog its
# environment names. Until the fork's master has that, run it with ARROYO_REF=feat/metalake-resolve.
#
#   bash crates/metalake-arroyo/e2e/run.sh
#   ARROYO_REF=feat/metalake-connector bash crates/metalake-arroyo/e2e/run.sh
#   KEEP=1 bash crates/metalake-arroyo/e2e/run.sh      leaves everything running, to look at
#   ARROYO_IMAGE=arroyo:local bash crates/metalake-arroyo/e2e/run.sh   an image already built
#
# Building Arroyo's image takes long the first time; Docker caches it after.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "$here/../../.." && pwd)
# Docker takes the host's paths: on Windows, Git Bash's /c/... as C:/...
if command -v cygpath >/dev/null 2>&1; then here=$(cygpath -m "$here"); root=$(cygpath -m "$root"); fi
ref=${ARROYO_REF:-master}
postgres=${POSTGRES_IMAGE:-postgres:17@sha256:3b0cda93be7c8dba2ed484d19393d93845dc3dafb88973e248ba8e6d99d784e1}
net=metalake-e2e; pg=metalake-e2e-pg; job=metalake-e2e-arroyo; lake=metalake-e2e-lake
# Git Bash would turn the containers' paths into Windows ones.
export MSYS_NO_PATHCONV=1

clean() {
  docker rm -f "$job" "$pg" >/dev/null 2>&1 || true
  docker volume rm "$lake" >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
}
clean
[ -n "${KEEP:-}" ] || trap clean EXIT

echo "== images"
arroyo=${ARROYO_IMAGE:-metalake-e2e/arroyo:${ref//\//-}}
if [ -z "${ARROYO_IMAGE:-}" ]; then
  docker build -q -t "$arroyo" --target arroyo -f docker/Dockerfile \
    "https://github.com/describeloai/arroyo.git#$ref" >/dev/null
fi
docker build -q -t metalake-e2e/metalake -f "$here/metalake.Dockerfile" "$root" >/dev/null

echo "== catalog"
docker network create "$net" >/dev/null
docker volume create "$lake" >/dev/null
docker run -d --name "$pg" --network "$net" -e POSTGRES_HOST_AUTH_METHOD=trust "$postgres" \
  -c track_commit_timestamp=on >/dev/null
until docker exec "$pg" pg_isready -q -U postgres 2>/dev/null; do sleep 1; done; sleep 2

metalake() {
  docker run --rm -i --network "$net" -v "$lake:/lake" -w /lake metalake-e2e/metalake "$@"
}
metalake init "postgresql://postgres@$pg/postgres" /lake/data --schema lake
metalake sql "create schema main.shop;
              create table main.shop.orders (id bigint, customer varchar, amount double);
              create table main.shop.totals (customer varchar, orders bigint, amount double);
              insert into main.shop.orders values (1, 'ana', 10), (2, 'ana', 20), (3, 'bo', 5), (4, 'cy', 7)"

# Arroyo reaches the lake as a client does, from its environment: the pipeline names no catalog of its own.
start() {
  docker run -d --name "$job" --network "$net" -v "$lake:/lake" -v "$here/pipeline.sql:/pipeline.sql:ro" \
    -e METALAKE_POSTGRES="host=$pg user=postgres dbname=postgres" -e METALAKE_SCHEMA=lake \
    "$arroyo" run --name totals --parallelism 2 --state-dir /lake/state /pipeline.sql >/dev/null
}

# The totals the orders give, and those the pipeline keeps, until they agree.
settled() {
  local want got
  want=$(metalake sql --format csv "select customer, count(*) as orders, sum(amount) as amount
                                     from main.shop.orders group by customer order by customer")
  for _ in $(seq 1 60); do
    got=$(metalake sql --format csv "select customer, orders, amount from main.shop.totals order by customer")
    if [ "$got" = "$want" ]; then
      echo "   $1: $(echo "$got" | tail -n +2 | tr '\n' ' ')"
      return 0
    fi
    sleep 2
  done
  echo "   $1: the totals never came to the orders'"
  echo "   want:"; echo "$want" | sed 's/^/     /'
  echo "   got:"; echo "$got" | sed 's/^/     /'
  docker logs --tail 80 "$job"
  return 1
}

echo "== pipeline"
start
settled "snapshot"
metalake sql "insert into main.shop.orders values (5, 'bo', 15), (6, 'dee', 30)"
settled "inserts"
metalake sql "delete from main.shop.orders where id = 2"
settled "an order deleted"
metalake sql "delete from main.shop.orders where customer = 'cy'"
settled "a customer gone"

echo "== restart"
docker stop -t 30 "$job" >/dev/null
docker rm "$job" >/dev/null
metalake sql "insert into main.shop.orders values (7, 'ana', 1), (8, 'eve', 2)"
start
settled "restored from its checkpoint"
metalake sql "delete from main.shop.orders where customer = 'dee'; insert into main.shop.orders values (9, 'bo', 4)"
settled "after the restart"

echo "== passed"
metalake log --table shop.totals | head -20
