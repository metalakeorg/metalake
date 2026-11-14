// A statement's rows, as `POST /api/v1/sql` answers them: an Arrow IPC stream, read with Flechette (ADR 0007 §4.1), so
// that each value keeps its type: a 64-bit integer as a `BigInt`, a timestamp as a `Date`.

import { tableFromIPC } from "@uwdata/flechette";

/** A statement's first rows, by column; and whether it answered more. */
export interface Result {
  columns: string[];
  /** Each row's values, in the columns' order. */
  rows: unknown[][];
  truncated: boolean;
}

/** The rows of an Arrow IPC stream, by position: two columns of one name stay two. */
export function decode(bytes: ArrayBuffer, truncated: boolean): Result {
  const table = tableFromIPC(new Uint8Array(bytes), { useBigInt: true, useDate: true });
  const columns = table.schema.fields.map((field) => field.name);
  const children = columns.map((_, index) => table.getChildAt(index));
  const rows: unknown[][] = [];
  for (let row = 0; row < table.numRows; row++) {
    rows.push(children.map((column) => column.at(row)));
  }
  return { columns, rows, truncated };
}

/** A value as the grid shows it: `NULL` for a null, a time in ISO 8601, a nested value as JSON. */
export function show(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Uint8Array) return `0x${[...value].map((b) => b.toString(16).padStart(2, "0")).join("")}`;
  if (typeof value === "object") {
    return JSON.stringify(value, (_, inner) => (typeof inner === "bigint" ? inner.toString() : inner));
  }
  return String(value);
}

/** A value as JSON has it: a 64-bit integer as a number while it is exact, else as its digits; a time in ISO 8601;
 * bytes as the grid says them. */
function plain(value: unknown): unknown {
  if (value === undefined) return null;
  if (typeof value === "bigint") {
    return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(value)
      : value.toString();
  }
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) return show(value);
  return value;
}

/** The rows as JSON: an array of objects by column name; a name that repeats takes its position after it (`k_2`). */
export function toJson(result: Result): string {
  const seen = new Map<string, number>();
  const keys = result.columns.map((name) => {
    const count = (seen.get(name) ?? 0) + 1;
    seen.set(name, count);
    return count === 1 ? name : `${name}_${count}`;
  });
  const objects = result.rows.map((row) => Object.fromEntries(keys.map((key, at) => [key, row[at]])));
  return JSON.stringify(objects, (_, value) => plain(value), 2);
}
