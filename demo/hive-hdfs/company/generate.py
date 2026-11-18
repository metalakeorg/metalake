"""Tienda's raw data, as its web and its systems drop it: seeded, so that every run writes the same bytes.

Each day is generated from its own seed, so one day alone is the same as that day within the whole history:

    python generate.py --out out                    # the 90 days
    python generate.py --out out --day 91           # one more day, the 91st

It writes CSV with no header, laid out as the landing zone on HDFS is, one directory per Hive partition:

    out/raw/clientes/dt=2024-01-01/clientes.csv      customers new that day (the first day: the initial 4,000)
    out/raw/productos/dt=2024-01-01/productos.csv    products new that day (the first day: the initial 450)
    out/raw/eventos/dt=2024-01-01/hora=06/eventos.csv  one drop of the web's events: views, carts, purchases
    out/raw/stock/dt=2024-01-01/stock.csv            each product's stock in each warehouse at the day's close

Products new from day 45 on have a brand (`marca`); before, the column is empty, as the source system had none.
"""

import argparse
import csv
import datetime
import random
from pathlib import Path

SEED = 20240101
START = datetime.date(2024, 1, 1)
DAYS = 90
DROPS = ["00", "06", "12", "18"]
BRAND_FROM_DAY = 45

INITIAL_CUSTOMERS = 4000
INITIAL_PRODUCTS = 450
EVENTS_PER_DROP = 5000
WAREHOUSES = ["MAD", "BCN", "VLC"]

SEGMENTS = ["particular"] * 7 + ["empresa"] * 2 + ["premium"]
COUNTRIES = ["ES"] * 8 + ["PT", "FR"]
CATEGORIES = ["hogar", "moda", "electronica", "deporte", "libros", "juguetes", "belleza", "alimentacion"]
BRANDS = ["Norte", "Brisa", "Atlas", "Lumen", "Vela", "Orbe"]
KINDS = ["vista"] * 70 + ["carrito"] * 15 + ["compra"] * 15


def rng_of(day: int, what: str) -> random.Random:
    """The generator of one day's one kind of data, the same whatever else is generated."""
    return random.Random(f"{SEED}:{day}:{what}")


def date_of(day: int) -> datetime.date:
    return START + datetime.timedelta(days=day - 1)


def customers_until(day: int) -> int:
    """How many customers exist at the end of `day`: the first 4,000, then each day's new ones."""
    return INITIAL_CUSTOMERS + sum(new_customers(d) for d in range(2, day + 1))


def new_customers(day: int) -> int:
    return rng_of(day, "customers-count").randint(5, 17)


def products_until(day: int) -> int:
    return INITIAL_PRODUCTS + sum(new_products(d) for d in range(2, day + 1))


def new_products(day: int) -> int:
    return 1 if day >= BRAND_FROM_DAY and rng_of(day, "products-count").random() < 0.55 else 0


def price_of(product: int) -> float:
    return round(random.Random(f"{SEED}:price:{product}").uniform(3, 300), 2)


def write(path: Path, rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        out = csv.writer(f, lineterminator="\n")
        for row in rows:
            out.writerow(row)
            count += 1
    return count


def customers(day: int):
    first = 1 if day == 1 else customers_until(day - 1) + 1
    last = customers_until(day)
    rng = rng_of(day, "customers")
    for cliente in range(first, last + 1):
        yield [cliente, f"Cliente {cliente}", rng.choice(SEGMENTS), rng.choice(COUNTRIES), date_of(day).isoformat()]


def products(day: int):
    first = 1 if day == 1 else products_until(day - 1) + 1
    last = products_until(day)
    rng = rng_of(day, "products")
    for producto in range(first, last + 1):
        marca = rng.choice(BRANDS) if day >= BRAND_FROM_DAY else ""
        yield [producto, f"Producto {producto}", rng.choice(CATEGORIES), price_of(producto), marca]


def events(day: int, drop: str):
    rng = rng_of(day, f"events-{drop}")
    clientes, productos = customers_until(day), products_until(day)
    base = datetime.datetime.combine(date_of(day), datetime.time(int(drop)))
    for n in range(EVENTS_PER_DROP):
        kind = rng.choice(KINDS)
        producto = rng.randint(1, productos)
        cantidad = rng.randint(1, 5) if kind == "compra" else ""
        precio = round(price_of(producto) * rng.choice([1, 1, 1, 0.9, 0.8]), 2) if kind == "compra" else ""
        ts = base + datetime.timedelta(seconds=rng.randrange(6 * 3600))
        evento = f"{date_of(day):%Y%m%d}{drop}{n:05d}"
        yield [evento, ts.isoformat(sep=" "), rng.randint(1, clientes), kind, producto, cantidad, precio]


def stock(day: int):
    rng = rng_of(day, "stock")
    for producto in range(1, products_until(day) + 1):
        for almacen in WAREHOUSES:
            yield [producto, almacen, rng.randint(0, 400)]
        # The warehouse system sometimes loses where a count was made: Hive keeps those as its null partition.
        if rng.random() < 0.01:
            yield [producto, "", rng.randint(0, 50)]


def generate(out: Path, day: int) -> dict:
    dt = f"dt={date_of(day).isoformat()}"
    raw = out / "raw"
    counts = {
        "clientes": write(raw / "clientes" / dt / "clientes.csv", customers(day)),
        "productos": write(raw / "productos" / dt / "productos.csv", products(day)),
        "stock": write(raw / "stock" / dt / "stock.csv", stock(day)),
        "eventos": 0,
    }
    for drop in DROPS:
        counts["eventos"] += write(raw / "eventos" / dt / f"hora={drop}" / "eventos.csv", events(day, drop))
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--day", type=int, help="one day only, 1 being 2024-01-01; all 90 when not given")
    args = parser.parse_args()
    days = [args.day] if args.day else range(1, DAYS + 1)
    total: dict = {}
    for day in days:
        for kind, count in generate(args.out, day).items():
            total[kind] = total.get(kind, 0) + count
    print(" ".join(f"{kind}={count}" for kind, count in sorted(total.items())), f"days={len(list(days))}")


if __name__ == "__main__":
    main()
