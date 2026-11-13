// The UI's API, typed from its OpenAPI spec (ADR 0007 §4.1): `schema.d.ts` is generated from `openapi.json`, which the
// router's own spec is compared with, so that a path or a field the server changes fails the type check here.

import createClient, { type Middleware } from "openapi-fetch";
import { token, Unauthorized } from "./auth";
import { decode, type Result } from "./result";
import type { components, paths } from "./schema";

export type Status = components["schemas"]["Status"];
export type Fault = components["schemas"]["Fault"];

/** A request the API answered with a failure: its kind and message, as the command line says them. */
export class Failure extends Error {
  readonly fault: Fault;

  constructor(fault: Fault) {
    super(fault.message);
    this.name = "Failure";
    this.fault = fault;
  }
}

/** Every request bears the run's token; a refusal of it is `Unauthorized`, which the frame says. */
const bearer: Middleware = {
  onRequest({ request }) {
    const given = token();
    if (given) request.headers.set("Authorization", `Bearer ${given}`);
    return request;
  },
  onResponse({ response }) {
    if (response.status === 401) throw new Unauthorized();
    return response;
  },
};

/** A client of the API, over `call`, which a test gives. */
export function client(call: typeof fetch = (request) => fetch(request)) {
  const api = createClient<paths>({ baseUrl: globalThis.location?.origin ?? "", fetch: (request) => call(request) });
  api.use(bearer);
  return api;
}

export type Client = ReturnType<typeof client>;

/** What the lake is. */
export async function status(api: Client = client()): Promise<Status> {
  const { data, error } = await api.GET("/api/v1/status");
  if (error) throw new Failure(error);
  return data;
}

export type Ref = components["schemas"]["Ref"];
export type Table = components["schemas"]["Table"];
export type Mark = components["schemas"]["Mark"];

/** Every branch and tag. */
export async function refs(api: Client = client()): Promise<Ref[]> {
  const { data, error } = await api.GET("/api/v1/refs");
  if (error) throw new Failure(error);
  return data;
}

/** The tables `on` sees at its head. */
export async function tables(on: string, api: Client = client()): Promise<Table[]> {
  const { data, error } = await api.GET("/api/v1/refs/{ref}/tables", { params: { path: { ref: on } } });
  if (error) throw new Failure(error);
  return data;
}

/** How many rows a statement answers to the page. */
export const LIMIT = 10_000;

/**
 * Runs a text's statements on the lake, names that say no ref on `branch`, and answers the first rows of the last. An
 * abort of `signal` drops the request, and the server stops the statement.
 */
export async function run(sql: string, branch: string, signal?: AbortSignal, api: Client = client()): Promise<Result> {
  const { data, error, response } = await api.POST("/api/v1/sql", {
    body: { sql, branch, limit: LIMIT },
    parseAs: "arrayBuffer",
    ...(signal ? { signal } : {}),
  });
  if (error) throw new Failure(error);
  return decode(data, response.headers.get("metalake-truncated") === "true");
}

export type NotebookState = components["schemas"]["Notebook"];

/** Whether this `metalake` can give a Notebook: a Python, and JupyterLab in it. */
export async function notebook(api: Client = client()): Promise<NotebookState> {
  const { data, error } = await api.GET("/api/v1/notebook");
  if (error) throw new Failure(error);
  return data;
}

/** Starts the UI's JupyterLab, unless it runs already, once it serves: what the Notebook can do then. */
export async function startNotebook(api: Client = client()): Promise<NotebookState> {
  const { data, error } = await api.POST("/api/v1/notebook/start");
  if (error) throw new Failure(error);
  return data;
}
