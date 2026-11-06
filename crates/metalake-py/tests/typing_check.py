"""Read by mypy and never run: what `stubtest` cannot see, as a compiled function does not say what it returns."""

from typing import assert_type

import polars

import metalake
from metalake._metalake import Batch, Commit, Done, Merged, Ref, Snapshot, Status, Written

lake = metalake.connect("./lake")
assert_type(lake, metalake.Lake)

result = lake.sql("select 1")
assert_type(result, metalake.Result)
assert_type(result.columns, list[str])
assert_type(result.to_polars(), polars.DataFrame)
for batch in result:
    assert_type(batch, Batch)

table = lake.table("t")
assert_type(table, metalake.Table)
assert_type(table.columns, list[str])
frame = polars.DataFrame({"k": [1]})
assert_type(table.append(frame, idempotent=("job", 1)), Written)
assert_type(table.append(result), Written)
assert_type(table.overwrite(frame), Written)
assert_type(table.upsert(frame, on=["k"]), Written)
assert_type(table.delete("k = 1"), Written)

assert_type(lake.status(), Status)
assert_type(lake.refs(), list[Ref])
assert_type(lake.branch("dev", start="main@1"), Ref)
assert_type(lake.log(), list[Commit])
assert_type(lake.log(table="default.t"), list[Snapshot])
assert_type(lake.merge("dev", check=True), Merged)
assert_type(lake.maintain("compact", table="default.t"), list[Done])

try:
    lake.sql("select 1")
except metalake.ConflictError as e:
    assert_type(e.rule, str | None)
    assert_type(e.retryable, bool)
    assert_type(e.done, list[Done])
