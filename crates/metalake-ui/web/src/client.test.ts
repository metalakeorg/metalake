import { columnFromArray, int64, tableFromColumns, tableToIPC } from "@uwdata/flechette";
import { afterEach, describe, expect, it } from "vitest";
import { adopt, forget, Unauthorized } from "./auth";
import { client, Failure, runSteps, type Status, status } from "./client";

const LAKE: Status = {
  schema: "metalake",
  local: "/home/a/lake/postgres",
  warehouse: "file:///home/a/lake/data",
  format_version: "0",
  writer_version: "0.2.0",
  protocol_version: "0",
  refs: [{ name: "main", kind: "branch" }],
};

/** A server that answers `body` with `code`, and what it was asked. */
function server(code: number, body: unknown) {
  const asked: Request[] = [];
  const call = async (request: RequestInfo | URL) => {
    asked.push(request as Request);
    return new Response(JSON.stringify(body), { status: code, headers: { "content-type": "application/json" } });
  };
  return { api: client(call as typeof fetch), asked };
}

describe("status", () => {
  afterEach(forget);

  it("asks with the run's token, and answers the lake", async () => {
    adopt({
      location: { hash: "#token=abc", pathname: "/", search: "" },
      history: { state: null, replaceState: () => {} },
      storage: null,
    });
    const { api, asked } = server(200, LAKE);
    expect(await status(api)).toEqual(LAKE);
    expect(new URL(asked[0]?.url ?? "").pathname).toBe("/api/v1/status");
    expect(asked[0]?.headers.get("Authorization")).toBe("Bearer abc");
  });

  it("fails with the API's kind and message", async () => {
    const { api } = server(502, { kind: "unknown", message: "the connection was lost", retryable: true });
    const failure = await status(api).catch((e: unknown) => e);
    expect(failure).toBeInstanceOf(Failure);
    expect((failure as Failure).fault.kind).toBe("unknown");
    expect((failure as Failure).message).toBe("the connection was lost");
  });

  it("is unauthorized without the run's token", async () => {
    const { api } = server(401, { kind: "usage", message: "no token", retryable: false });
    await expect(status(api)).rejects.toBeInstanceOf(Unauthorized);
  });
});

/** A server that answers a run's lines as `chunks`, each sent once `next()` is called: a chunk may end within a line. */
function streaming(chunks: string[]) {
  const asked: Request[] = [];
  let push: (() => void) | undefined;
  let sent = 0;
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      push = () => {
        const chunk = chunks[sent++];
        if (chunk === undefined) controller.close();
        else controller.enqueue(new TextEncoder().encode(chunk));
      };
    },
  });
  const call = async (request: RequestInfo | URL) => {
    asked.push(request as Request);
    return new Response(body, { status: 200, headers: { "content-type": "application/x-ndjson" } });
  };
  return { api: client(call as typeof fetch), asked, next: () => push?.() };
}

/** Arrow IPC of one column `n`, in base64, as a run's line holds a read's rows. */
function rowsOf(values: bigint[]): string {
  const bytes = tableToIPC(tableFromColumns({ n: columnFromArray(values, int64()) }), {
    format: "stream",
  }) as Uint8Array;
  return btoa(String.fromCharCode(...bytes));
}

describe("runSteps", () => {
  it("gives each statement as its line comes, before the run ends", async () => {
    const read = JSON.stringify({
      index: 0,
      sql: "select n",
      state: "ok",
      ms: 3,
      rows: 2,
      truncated: true,
      arrow: rowsOf([1n, 2n]),
    });
    const write = JSON.stringify({
      index: 1,
      sql: "insert",
      state: "ok",
      engine: "duckdb",
      ms: 40,
      written: { commit: 57, rows: 1 },
    });
    const failed = JSON.stringify({
      index: 2,
      sql: "select nope",
      state: "error",
      ms: 1,
      fault: { kind: "usage", message: "no column nope", retryable: false },
    });
    const skipped = JSON.stringify({ index: 3, sql: "drop table t", state: "skipped" });
    // The first line split in two chunks, the rest in one.
    const { api, asked, next } = streaming([
      read.slice(0, 20),
      `${read.slice(20)}\n`,
      `${write}\n${failed}\n${skipped}\n`,
    ]);
    const steps = runSteps("select n; …", "dev", undefined, api);

    next();
    next();
    const first = await steps.next();
    expect(first.done).toBe(false);
    expect(first.value).toMatchObject({ index: 0, sql: "select n", state: "ok", ms: 3 });
    expect(first.value?.result?.columns).toEqual(["n"]);
    expect(first.value?.result?.rows).toEqual([[1n], [2n]]);
    expect(first.value?.result?.truncated).toBe(true);
    expect(await asked[0]?.clone().json()).toEqual({ sql: "select n; …", branch: "dev", limit: 10_000 });
    expect(new URL(asked[0]?.url ?? "").pathname).toBe("/api/v1/sql/run");

    next();
    next();
    const rest = [];
    for await (const step of steps) rest.push(step);
    expect(rest.map((s) => s.state)).toEqual(["ok", "error", "skipped"]);
    expect(rest[0]?.written).toEqual({ commit: 57, rows: 1 });
    expect(rest[0]?.engine).toBe("duckdb");
    expect(first.value?.engine).toBeUndefined();
    expect(rest[0]?.result).toBeUndefined();
    expect(rest[1]?.fault?.message).toBe("no column nope");
    expect(rest[2]?.ms).toBeUndefined();
  });

  it("fails as a whole with the fault of a text it refuses", async () => {
    const { api } = server(400, { kind: "usage", message: "no statement to run", retryable: false });
    const steps = runSteps("  ", "main", undefined, api);
    const failure = await steps.next().catch((e: unknown) => e);
    expect(failure).toBeInstanceOf(Failure);
    expect((failure as Failure).message).toBe("no statement to run");
  });
});
