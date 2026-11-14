import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Frame } from "./Frame";
import { ask } from "./server";

describe("Frame", () => {
  it("names the product", () => {
    render(<Frame />);
    expect(screen.getByRole("banner")).toHaveTextContent("Metalake UI");
  });

  it("renders the page in main when the server is ready", () => {
    render(<Frame server={{ state: "ready", api: 1 }}>a page</Frame>);
    expect(screen.getByRole("main")).toHaveTextContent("a page");
  });

  it("asks for the run's URL when the token is refused", () => {
    render(<Frame server={{ state: "unauthorized" }}>a page</Frame>);
    expect(screen.getByRole("alert")).toHaveTextContent("lacks the run's token");
    expect(screen.queryByText("a page")).toBeNull();
  });

  it("says a server of another version", () => {
    render(<Frame server={{ state: "other", api: 2 }} />);
    expect(screen.getByRole("alert")).toHaveTextContent("version 2");
  });
});

describe("ask", () => {
  const answer = (status: number, body: unknown) => async () => new Response(JSON.stringify(body), { status });

  it("ready on this API's version", async () => {
    expect(await ask(answer(200, { api: 1 }))).toEqual({ state: "ready", api: 1 });
  });

  it("other on another", async () => {
    expect(await ask(answer(200, { api: 2 }))).toEqual({ state: "other", api: 2 });
  });

  it("unauthorized on 401", async () => {
    expect(await ask(answer(401, {}))).toEqual({ state: "unauthorized" });
  });

  it("unreachable when the call fails", async () => {
    const failing = async (): Promise<Response> => {
      throw new TypeError("Failed to fetch");
    };
    expect(await ask(failing)).toEqual({ state: "unreachable", message: "Failed to fetch" });
  });
});
