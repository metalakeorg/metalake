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

export type RefDetails = components["schemas"]["RefDetails"];
export type Commit = components["schemas"]["Commit"];
export type SchemaDetails = components["schemas"]["SchemaDetails"];
export type TableDetails = components["schemas"]["TableDetails"];
export type Snapshot = components["schemas"]["Snapshot"];
export type Column = components["schemas"]["Column"];

/** A schema's levels as a path names them: joined by U+001F, as Iceberg's REST catalog does. */
export function schemaPath(namespace: readonly string[]): string {
  return namespace.join("");
}

/** A ref: what it is, what it holds, and its schemas. */
export async function refDetails(on: string, api: Client = client()): Promise<RefDetails> {
  const { data, error } = await api.GET("/api/v1/refs/{ref}", { params: { path: { ref: on } } });
  if (error) throw new Failure(error);
  return data;
}

/** A ref's commits, the latest first. */
export async function commits(on: string, limit?: number, api: Client = client()): Promise<Commit[]> {
  const { data, error } = await api.GET("/api/v1/refs/{ref}/commits", {
    params: { path: { ref: on }, query: limit === undefined ? {} : { limit } },
  });
  if (error) throw new Failure(error);
  return data;
}

/** A schema of a ref, by its levels: its properties and its tables. */
export async function schemaDetails(
  on: string,
  namespace: readonly string[],
  api: Client = client(),
): Promise<SchemaDetails> {
  const { data, error } = await api.GET("/api/v1/refs/{ref}/schemas/{schema}", {
    params: { path: { ref: on, schema: schemaPath(namespace) } },
  });
  if (error) throw new Failure(error);
  return data;
}

/** A table of a ref: its columns, layout, size and Iceberg state. */
export async function tableDetails(
  on: string,
  namespace: readonly string[],
  table: string,
  api: Client = client(),
): Promise<TableDetails> {
  const { data, error } = await api.GET("/api/v1/refs/{ref}/tables/{schema}/{table}", {
    params: { path: { ref: on, schema: schemaPath(namespace), table } },
  });
  if (error) throw new Failure(error);
  return data;
}

/** A table's snapshots on a ref, the latest first. */
export async function snapshots(
  on: string,
  namespace: readonly string[],
  table: string,
  limit?: number,
  api: Client = client(),
): Promise<Snapshot[]> {
  const { data, error } = await api.GET("/api/v1/refs/{ref}/tables/{schema}/{table}/snapshots", {
    params: { path: { ref: on, schema: schemaPath(namespace), table }, query: limit === undefined ? {} : { limit } },
  });
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
