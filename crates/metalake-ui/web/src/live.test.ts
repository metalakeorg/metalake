import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";
import { follow, marks, Parser, same } from "./live";

describe("Parser", () => {
  it("splits events however the text is cut, and skips comments", () => {
    const parser = new Parser();
    expect(parser.push('event: mark\ndata: {"seq":1,')).toEqual([]);
    expect(parser.push('"recent":1,"deleted":0}\n\n:keep-alive\n\nevent: mark\r\ndata: {}\r\n\r\n')).toEqual([
      { event: "mark", data: '{"seq":1,"recent":1,"deleted":0}' },
      { event: "mark", data: "{}" },
    ]);
  });
});

/** A server whose events are `text`, then end. */
const serving =
  (text: string, status = 200) =>
  async () =>
    new Response(text, { status, headers: { "content-type": "text/event-stream" } });

describe("marks", () => {
  it("says each mark, and fails on a fault", async () => {
    const seen: number[] = [];
    const text =
      'event: mark\ndata: {"seq":1,"recent":1,"deleted":0}\n\nevent: mark\ndata: {"seq":2,"recent":1,"deleted":0}\n\n' +
      'event: fault\ndata: {"kind":"unknown","message":"lost","retryable":false}\n\n';
    await expect(marks((m) => seen.push(m.seq), new AbortController().signal, serving(text))).rejects.toThrow("lost");
    expect(seen).toEqual([1, 2]);
  });
});

describe("follow", () => {
  it("makes the catalog stale when the mark moves, not on the first", async () => {
    const queries = new QueryClient();
    const invalidate = vi.spyOn(queries, "invalidateQueries");
    const stop = new AbortController();
    const text =
      'event: mark\ndata: {"seq":1,"recent":1,"deleted":0}\n\nevent: mark\ndata: {"seq":1,"recent":1,"deleted":1}\n\n';
    const call = vi.fn(async () => {
      stop.abort();
      return serving(text)();
    });
    await follow(queries, stop.signal, call);
    expect(invalidate).toHaveBeenCalledTimes(1);
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ["catalog"] });
  });

  it("stops on a token refused", async () => {
    const call = vi.fn(serving("", 401));
    await follow(new QueryClient(), new AbortController().signal, call);
    expect(call).toHaveBeenCalledTimes(1);
  });
});

describe("same", () => {
  it("compares every part", () => {
    expect(same(null, { seq: 1, recent: 1, deleted: 0 })).toBe(false);
    expect(same({ seq: 1, recent: 1, deleted: 0 }, { seq: 1, recent: 1, deleted: 0 })).toBe(true);
    expect(same({ seq: 1, recent: 1, deleted: 0 }, { seq: 1, recent: 1, deleted: 1 })).toBe(false);
    // A commit that landed after a later one: the latest sequence is the same, the count of recent ones is not.
    expect(same({ seq: 2, recent: 1, deleted: 0 }, { seq: 2, recent: 2, deleted: 0 })).toBe(false);
  });
});
