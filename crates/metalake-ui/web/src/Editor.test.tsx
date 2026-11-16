import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Editor, VERBS } from "./Editor";

/** The words the editor colours as keywords in `text`, lowercase. */
function keywords(text: string): string[] {
  const { container } = render(<Editor value={text} onChange={() => {}} onRun={() => {}} />);
  return [...container.querySelectorAll(".tok-keyword")].map((e) => e.textContent?.toLowerCase() ?? "");
}

describe("Editor", () => {
  it("colours MetaLake's verbs as SQL's keywords", () => {
    const found = keywords(
      "create branch dev; show branches; merge branch dev dry run; use tag v1; show log limit 2;\n" +
        "alter lake grace 7 days; create pipeline p as insert into t select 1; using duckdb select 1",
    );
    for (const word of ["create", "branch", "show", "branches", "merge", "dry", "run", "use", "tag", "log"]) {
      expect(found).toContain(word);
    }
    for (const word of ["limit", "alter", "lake", "grace", "days", "pipeline", "insert", "using", "select"]) {
      expect(found).toContain(word);
    }
  });

  it("keeps each verb a keyword", () => {
    const words = VERBS.split(" ");
    expect(keywords(words.join(" "))).toEqual(words);
  });
});
