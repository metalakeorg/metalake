// The UI's API, typed from its OpenAPI spec (ADR 0007 §4.1): `schema.d.ts` is generated from `openapi.json`, which the
// router's own spec is compared with, so that a path or a field the server changes fails the type check here.

import createClient, { type Middleware } from "openapi-fetch";
import { token, Unauthorized } from "./auth";
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
