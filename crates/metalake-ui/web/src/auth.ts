// The run's token on the frontend's side (ADR 0007 §6.3). `metalake ui` opens the browser at `/#token=…`: the fragment
// never reaches a server, so the token is in no log and no `Referer`. The frontend keeps it in `localStorage`, which a
// browser isolates by origin, and so by port, unlike cookies, which every service on the loopback shares; takes it out
// of the address bar; and sends it as a bearer with every request to the API.

/** Where the token is kept, in this origin's storage. */
export const KEY = "metalake.token";

/** A request the server refused for want of this run's token: a token of a past run, or none. */
export class Unauthorized extends Error {
  constructor() {
    super("this page lacks the token of the running `metalake ui`");
    this.name = "Unauthorized";
  }
}

/** What reading the address and keeping the token needs, which a test gives. */
export interface Place {
  location: Pick<Location, "hash" | "pathname" | "search">;
  history: Pick<History, "replaceState" | "state">;
  storage: Pick<Storage, "getItem" | "setItem"> | null;
}

/** The browser's own, or none when it refuses its storage. */
export function place(): Place {
  let storage: Storage | null = null;
  try {
    storage = window.localStorage;
  } catch {
    // A browser that refuses storage: the token then lives in the page alone.
  }
  return { location: window.location, history: window.history, storage };
}

/**
 * Takes the token from the address's fragment, if it has one: keeps it, and leaves the address without it. Answers
 * the token to use: the one the address gave, or the one kept from before.
 */
export function adopt(at: Place = place()): string | null {
  const given = new URLSearchParams(at.location.hash.replace(/^#/, "")).get("token");
  if (given) {
    try {
      at.storage?.setItem(KEY, given);
    } catch {
      // Kept in the page alone.
    }
    at.history.replaceState(at.history.state, "", at.location.pathname + at.location.search);
    remembered = given;
    return given;
  }
  try {
    remembered = at.storage?.getItem(KEY) ?? remembered;
  } catch {
    // Nothing kept.
  }
  return remembered;
}

/** The token of this page: the one `adopt` found. */
let remembered: string | null = null;

/** Calls the UI's API with the run's token; refuses with `Unauthorized` when the server does. */
export async function api(path: string, init: RequestInit = {}, call: typeof fetch = fetch): Promise<Response> {
  const headers = new Headers(init.headers);
  if (remembered) {
    headers.set("Authorization", `Bearer ${remembered}`);
  }
  const response = await call(`/api${path}`, { ...init, headers });
  if (response.status === 401) {
    throw new Unauthorized();
  }
  return response;
}

/** Forgets the token, for a test. */
export function forget(): void {
  remembered = null;
}
