import { useEffect, useState } from "react";
import { api, Unauthorized } from "./auth";

/** The API's version this frontend was built against; a server of another refuses to be misread (ADR 0007 §4.1). */
export const API = 1;

/** What the frontend knows of the server it was served by. */
export type Server =
  | { state: "asking" }
  | { state: "ready"; api: number }
  | { state: "unauthorized" }
  | { state: "other"; api: number }
  | { state: "unreachable"; message: string };

/** Asks the server its API's version, once. */
export function useServer(call: typeof fetch = fetch): Server {
  const [server, setServer] = useState<Server>({ state: "asking" });
  useEffect(() => {
    let current = true;
    ask(call).then((answer) => {
      if (current) setServer(answer);
    });
    return () => {
      current = false;
    };
  }, [call]);
  return server;
}

export async function ask(call: typeof fetch = fetch): Promise<Server> {
  try {
    const response = await api("/v1/version", {}, call);
    if (!response.ok) {
      return { state: "unreachable", message: `the server answered ${response.status}` };
    }
    const { api: version } = (await response.json()) as { api: number };
    return version === API ? { state: "ready", api: version } : { state: "other", api: version };
  } catch (error) {
    if (error instanceof Unauthorized) {
      return { state: "unauthorized" };
    }
    return { state: "unreachable", message: error instanceof Error ? error.message : String(error) };
  }
}
