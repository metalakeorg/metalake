import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import license from "rollup-plugin-license";
import { defineConfig } from "vitest/config";

// In development, `/api` goes to a running `metalake ui --no-browser`, whose origin `METALAKE_UI_URL` names (no path,
// no token). Started with `METALAKE_UI_TOKEN` set, the dev server's page is opened at `/#token=<the same>`.
const api = new URL(process.env.METALAKE_UI_URL ?? "http://127.0.0.1:19100").origin;

/** The licenses the bundle may carry: permissive ones, whose notices `licenses.txt` keeps. Any other fails the build. */
const ALLOWED = "(MIT OR Apache-2.0 OR BSD-2-Clause OR BSD-3-Clause OR ISC OR 0BSD)";

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
    // The third-party code the bundle carries, with each license's text, beside it: served at `/licenses.txt` and
    // copied into the wheel (ADR 0007 §5.2). Only what the bundle holds, not what builds it.
    license({
      thirdParty: {
        includePrivate: false,
        allow: { test: ALLOWED, failOnUnlicensed: true, failOnViolation: true },
        output: { file: fileURLToPath(new URL("dist/licenses.txt", import.meta.url)) },
      },
    }),
  ],
  build: {
    // Every asset is named by its hash, so the router may cache it forever (ADR 0007 §4.3).
    assetsDir: "assets",
    // Read by the browser of the user's machine, never an old one.
    target: "es2023",
    sourcemap: false,
  },
  server: {
    host: "127.0.0.1",
    proxy: {
      "/api": {
        target: api,
        // The UI refuses another Host, and a write from another Origin (ADR 0007 §4.2): the proxy speaks as its page.
        changeOrigin: true,
        configure: (proxy) => {
          proxy.on("proxyReq", (request) => {
            if (request.getHeader("origin")) request.setHeader("origin", api);
          });
        },
      },
      // The Notebook's JupyterLab (ADR 0008 §5), and its kernels' websockets: as the UI's page, whose origin Jupyter
      // compares with the host it is asked by.
      "/jupyter": {
        target: api,
        ws: true,
        changeOrigin: true,
        configure: (proxy) => {
          proxy.on("proxyReq", (request) => {
            if (request.getHeader("origin")) request.setHeader("origin", api);
          });
          proxy.on("proxyReqWs", (request) => {
            if (request.getHeader("origin")) request.setHeader("origin", api);
          });
        },
      },
    },
  },
  test: {
    environment: "jsdom",
    // The end-to-end tests are Playwright's, under e2e/.
    include: ["src/**/*.test.{ts,tsx}"],
    setupFiles: ["src/test/setup.ts"],
  },
});
