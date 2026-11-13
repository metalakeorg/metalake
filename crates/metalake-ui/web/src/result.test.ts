import { columnFromArray, int64, tableFromColumns, tableToIPC, timestamp, utf8 } from "@uwdata/flechette";
import { describe, expect, it } from "vitest";
import { decode, show } from "./result";

/** An Arrow IPC stream of `columns`, as the server writes one. */
function arrow(columns: Record<string, ReturnType<typeof columnFromArray>>): ArrayBuffer {
  const bytes = tableToIPC(tableFromColumns(columns), { format: "stream" }) as Uint8Array;
  return bytes.slice().buffer;
}

describe("decode", () => {
  it("reads each value with its type, by position", () => {
    const bytes = arrow({
      k: columnFromArray([1n, 9007199254740993n], int64()),
      s: columnFromArray(["a", null], utf8()),
      at: columnFromArray([Date.UTC(2026, 10, 13), null], timestamp()),
    });
    const result = decode(bytes, true);
    expect(result.columns).toEqual(["k", "s", "at"]);
    expect(result.truncated).toBe(true);
    // A 64-bit integer past 2^53 keeps every digit.
    expect(result.rows[1]?.[0]).toBe(9007199254740993n);
    expect(result.rows[1]?.[1]).toBeNull();
    expect(result.rows[0]?.[2]).toBeInstanceOf(Date);
  });
});

describe("show", () => {
  it("says each kind of value as the grid shows it", () => {
    expect(show(null)).toBe("NULL");
    expect(show(9007199254740993n)).toBe("9007199254740993");
    expect(show(new Date(Date.UTC(2026, 10, 13)))).toBe("2026-11-13T00:00:00.000Z");
    expect(show({ a: 1n, b: [1, 2] })).toBe('{"a":"1","b":[1,2]}');
    expect(show(new Uint8Array([0, 255]))).toBe("0x00ff");
    expect(show(1.5)).toBe("1.5");
  });
});
