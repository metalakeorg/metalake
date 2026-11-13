// The catalog as it is: the server's events say each time the lake's mark moves (`GET /api/v1/events`, server-sent
// events), and what the page shows of the catalog is read again. Read with `fetch`, not `EventSource`, which cannot
// send the run's token as a header; a connection that ends is opened again, and a mark that moved meanwhile is noticed
// by the first event of the next, which is the mark of the moment.

import { type QueryClient, useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";
import { token, Unauthorized } from "./auth";
import type { Fault, Mark } from "./client";

/** The queries of the catalog, which a moved mark makes stale. */
export const CATALOG = ["catalog"] as const;

/** How long to wait before opening the events again once they end. */
const AGAIN_MS = 2000;

/** One server-sent event: its name and its data. */
export interface Sent {
  event: string;
  data: string;
}

/** Splits a stream of server-sent events' text into events, as its chunks come, cut anywhere. */
export class Parser {
  private buffer = "";

  push(chunk: string): Sent[] {
    this.buffer += chunk.replace(/\r\n?/g, "\n");
    const sent: Sent[] = [];
    let end = this.buffer.indexOf("\n\n");
    while (end >= 0) {
      const block = this.buffer.slice(0, end);
      this.buffer = this.buffer.slice(end + 2);
      let event = "message";
      const data: string[] = [];
      for (const line of block.split("\n")) {
        // A line starting with a colon is a comment: the server's keep-alive.
        if (line.startsWith("event:")) event = line.slice(6).trim();
        else if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
      }
      if (data.length > 0) sent.push({ event, data: data.join("\n") });
      end = this.buffer.indexOf("\n\n");
    }
    return sent;
  }
}

/** The lake's marks, each as it comes, until the events end, fail, or `signal` aborts them. */
export async function marks(
  onMark: (mark: Mark) => void,
  signal: AbortSignal,
  call: typeof fetch = fetch,
): Promise<void> {
  const headers = new Headers({ Accept: "text/event-stream" });
  const given = token();
  if (given) headers.set("Authorization", `Bearer ${given}`);
  const response = await call("/api/v1/events", { headers, signal });
  if (response.status === 401) throw new Unauthorized();
  if (!response.ok || !response.body) throw new Error(`the events answered ${response.status}`);
  const parser = new Parser();
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return;
    for (const sent of parser.push(value)) {
      if (sent.event === "mark") onMark(JSON.parse(sent.data) as Mark);
      if (sent.event === "fault") throw new Error((JSON.parse(sent.data) as Fault).message);
    }
  }
}

/** Whether two marks are the same state of the lake. */
export function same(a: Mark | null, b: Mark): boolean {
  return a !== null && a.seq === b.seq && a.recent === b.recent && a.deleted === b.deleted;
}

/** Keeps `queries`' catalog as it is, for as long as `live` holds; reads the events again when they end. */
export async function follow(queries: QueryClient, signal: AbortSignal, call: typeof fetch = fetch): Promise<void> {
  let last: Mark | null = null;
  while (!signal.aborted) {
    try {
      await marks(
        (mark) => {
          if (last !== null && !same(last, mark)) void queries.invalidateQueries({ queryKey: CATALOG });
          last = mark;
        },
        signal,
        call,
      );
    } catch (error) {
      // A token refused stays refused: the frame says so, and nothing is asked again.
      if (error instanceof Unauthorized || signal.aborted) return;
    }
    await new Promise((resolve) => setTimeout(resolve, AGAIN_MS));
  }
}

/** Follows the lake's events while the page is open. */
export function useLive(enabled: boolean): void {
  const queries = useQueryClient();
  useEffect(() => {
    if (!enabled) return;
    const stop = new AbortController();
    void follow(queries, stop.signal);
    return () => stop.abort();
  }, [enabled, queries]);
}
