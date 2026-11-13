import { afterEach, describe, expect, it } from "vitest";
import { adopt, forget, Unauthorized } from "./auth";
import { client, Failure, type Status, status } from "./client";

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
