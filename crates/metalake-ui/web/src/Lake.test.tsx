import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Failure, type Status } from "./client";
import { Frame } from "./Frame";
import { Failed, LakeView } from "./Lake";

const OWN: Status = {
  schema: "metalake",
  local: "/home/a/lake/postgres",
  warehouse: "file:///home/a/lake/data",
  format_version: "0",
  writer_version: "0.2.0",
  protocol_version: null,
  refs: [
    { name: "main", kind: "branch" },
    { name: "v1", kind: "tag" },
  ],
};

describe("LakeView", () => {
  it("says what the lake is, and its refs", () => {
    render(<LakeView lake={OWN} />);
    expect(document.body).toHaveTextContent("own Postgres, /home/a/lake/postgres");
    expect(document.body).toHaveTextContent("file:///home/a/lake/data");
    expect(document.body).toHaveTextContent("Protocolnone");
    const refs = screen.getByRole("list", { name: "Refs" });
    expect(refs).toHaveTextContent("mainbranch");
    expect(refs).toHaveTextContent("v1tag");
  });
});

describe("Frame on a lake", () => {
  it("names the lake by its schema and its catalog", () => {
    render(<Frame server={{ state: "ready", api: 1 }} lake={{ ...OWN, local: null, postgres: "host=db user=u" }} />);
    expect(screen.getByRole("banner")).toHaveTextContent("metalake·host=db user=u");
  });

  it("shows no navigation while the server keeps the page from showing", () => {
    render(<Frame server={{ state: "unauthorized" }} nav={<nav aria-label="Pages" />} />);
    expect(screen.queryByRole("navigation")).toBeNull();
  });
});

describe("Failed", () => {
  it("says the failure as the command line does", () => {
    render(<Failed error={new Failure({ kind: "failed", message: "the catalog said no", retryable: false })} />);
    expect(screen.getByRole("alert")).toHaveTextContent("could not be read (failed)");
    expect(screen.getByRole("alert")).toHaveTextContent("metalake: the catalog said no");
  });
});
