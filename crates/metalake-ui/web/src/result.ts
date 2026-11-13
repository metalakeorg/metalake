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
