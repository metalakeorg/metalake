"""What a client takes to load `db.long`, as `z5_window_cost` asks: the milliseconds of each load and the bytes of the last."""

import json
import sys
import time
import urllib.request

uri, times = sys.argv[1], int(sys.argv[2])
url = f"{uri}/v1/namespaces/db/tables/long"
taken, size = [], 0
for _ in range(times):
    start = time.perf_counter()
    with urllib.request.urlopen(url, timeout=3600) as answer:
        size = len(answer.read())
    taken.append((time.perf_counter() - start) * 1000)
print(json.dumps({"ms": taken, "bytes": size}))
