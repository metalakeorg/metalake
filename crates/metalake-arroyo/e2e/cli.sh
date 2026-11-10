#!/usr/bin/env bash
# A continuous pipeline from the command line, as a person drives one: created with `metalake pipeline create`,
# suspended in SQL, resumed, listed and dropped, Arroyo in a container of its image (the client's own runner). The
# totals it keeps must come to what the orders give after each change. Needs Docker, and `metalake` built for this
# machine: on Linux, as the end-to-end workflow runs it.
#
#   METALAKE=target/release/metalake bash crates/metalake-arroyo/e2e/cli.sh
set -euo pipefail
metalake=$(realpath "${METALAKE:-target/release/metalake}")
image=${POSTGRES_IMAGE:-postgres:17@sha256:3b0cda93be7c8dba2ed484d19393d93845dc3dafb88973e248ba8e6d99d784e1}
pg=metalake-e2e-cli-pg; port=${PORT:-55434}
dir=$(mktemp -d)

clean() {
  "$metalake" pipeline drop totals >/dev/null 2>&1 || true
  docker rm -f "$pg" >/dev/null 2>&1 || true
}
cd "$dir"
trap clean EXIT

docker rm -f "$pg" >/dev/null 2>&1 || true
docker run -d --name "$pg" -p "$port:5432" -e POSTGRES_HOST_AUTH_METHOD=trust "$image" -c track_commit_timestamp=on >/dev/null
until docker exec "$pg" pg_isready -q -U postgres 2>/dev/null; do sleep 1; done; sleep 2

"$metalake" init "host=localhost port=$port user=postgres dbname=postgres" "$dir/data" --schema cli >/dev/null
"$metalake" sql "create schema main.shop;
                 create table main.shop.orders (id bigint, customer varchar, amount double);
                 create table main.shop.totals (customer varchar, orders bigint, amount double);
                 insert into main.shop.orders values (1, 'ana', 10), (2, 'ana', 20), (3, 'bo', 5)" >/dev/null

settled() {
  local want got
  want=$("$metalake" sql --format csv "select customer, count(*) as orders, sum(amount) as amount
                                        from main.shop.orders group by customer order by customer")
  for _ in $(seq 1 90); do
    got=$("$metalake" sql --format csv "select customer, orders, amount from main.shop.totals order by customer")
    if [ "$got" = "$want" ]; then echo "   $1: $(echo "$got" | tail -n +2 | tr '\n' ' ')"; return 0; fi
    sleep 2
  done
  echo "   $1: the totals never came to the orders'"; echo "want: $want"; echo "got: $got"
  "$metalake" pipeline describe totals
  return 1
}

echo "== create"
"$metalake" pipeline create totals --query "INSERT INTO shop.totals
  SELECT customer, count(*) AS orders, sum(amount) AS amount FROM shop.orders GROUP BY customer"
settled "snapshot"
"$metalake" sql "insert into main.shop.orders values (4, 'cy', 7)" >/dev/null
settled "an insert"

echo "== suspend, in SQL"
"$metalake" sql "alter pipeline totals suspend"
"$metalake" sql "insert into main.shop.orders values (5, 'bo', 1); delete from main.shop.orders where id = 1" >/dev/null
echo "== resume"
"$metalake" pipeline resume totals
settled "resumed"

echo "== list"
"$metalake" sql --format csv "show pipelines" | cut -d, -f1,2
"$metalake" pipeline drop totals
[ -z "$("$metalake" sql --format csv "show pipelines" | tail -n +2)" ] || { echo "   still listed"; exit 1; }
echo "== passed"
