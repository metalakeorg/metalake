// After `vite build`: writes a `.br` and a `.gz` beside each file of `dist/` worth compressing, so that the router
// answers the one a request accepts, compressed once at build and not per request (ADR 0007 §4.3); and puts back the
// `.gitkeep` that keeps the folder.

import { readdir, readFile, stat, writeFile } from "node:fs/promises";
import { extname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { brotliCompressSync, constants, gzipSync } from "node:zlib";

const DIST = fileURLToPath(new URL("../dist/", import.meta.url));
const TEXT = new Set([".html", ".js", ".mjs", ".css", ".svg", ".json", ".txt", ".map", ".wasm"]);
/** Below this, the headers weigh more than what compression saves. */
const MIN = 1024;

async function* files(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) yield* files(path);
    else yield path;
  }
}

let count = 0;
for await (const path of files(DIST)) {
  if (!TEXT.has(extname(path)) || (await stat(path)).size < MIN) continue;
  const bytes = await readFile(path);
  const br = brotliCompressSync(bytes, {
    params: {
      [constants.BROTLI_PARAM_QUALITY]: constants.BROTLI_MAX_QUALITY,
      [constants.BROTLI_PARAM_SIZE_HINT]: bytes.length,
    },
  });
  const gz = gzipSync(bytes, { level: constants.Z_BEST_COMPRESSION });
  // A compressed file no smaller is left out: the router then answers the file itself.
  if (br.length < bytes.length) await writeFile(`${path}.br`, br);
  if (gz.length < bytes.length) await writeFile(`${path}.gz`, gz);
  count++;
}
console.log(`compressed ${count} files in dist/`);

// `vite build` empties `dist/`; the file that keeps the folder in git, which `rust-embed` needs to compile, goes back.
await writeFile(join(DIST, ".gitkeep"), "");
