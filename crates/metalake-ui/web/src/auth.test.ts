import { afterEach, describe, expect, it, vi } from "vitest";
import { adopt, api, forget, KEY, type Place, Unauthorized } from "./auth";

/** An address and a storage of a test's own. */
function at(hash: string, kept: Record<string, string> = {}): Place & { kept: Record<string, string>; url: string[] } {
  const kept_ = { ...kept };
  const url: string[] = [];
  return {
    location: { hash, pathname: "/refs/main", search: "?a=1" },
    history: { state: null, replaceState: (_state, _title, to) => url.push(String(to)) },
    storage: {
      getItem: (key) => kept_[key] ?? null,
      setItem: (key, value) => {
        kept_[key] = value;
      },
    },
    kept: kept_,
    url,
  };
}

afterEach(forget);

describe("adopt", () => {
  it("keeps the fragment's token and takes it out of the address", () => {
    const place = at("#token=abc123");
    expect(adopt(place)).toBe("abc123");
    expect(place.kept[KEY]).toBe("abc123");
    expect(place.url).toEqual(["/refs/main?a=1"]);
  });

  it("uses the token kept when the address has none", () => {
    const place = at("", { [KEY]: "kept" });
    expect(adopt(place)).toBe("kept");
    expect(place.url).toEqual([]);
  });

  it("a fragment's token replaces the kept one", () => {
    const place = at("#token=new", { [KEY]: "old" });
    expect(adopt(place)).toBe("new");
    expect(place.kept[KEY]).toBe("new");
  });

  it("works without storage", () => {
    expect(adopt({ ...at("#token=t"), storage: null })).toBe("t");
  });
});

describe("api", () => {
  it("bears the token", async () => {
    adopt(at("#token=abc123"));
    const call = vi.fn(async () => new Response("{}"));
    await api("/v1/version", {}, call);
    const [path, init] = call.mock.calls[0] as unknown as [string, RequestInit];
    expect(path).toBe("/api/v1/version");
    expect(new Headers(init.headers).get("Authorization")).toBe("Bearer abc123");
  });

  it("refuses with Unauthorized on 401", async () => {
    const call = vi.fn(async () => new Response("{}", { status: 401 }));
    await expect(api("/v1/version", {}, call)).rejects.toBeInstanceOf(Unauthorized);
  });
});
