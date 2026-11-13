import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// In development, `/api` goes to a running `metalake ui --no-browser`, whose origin `METALAKE_UI_URL` names (no path,
// no token). Started with `METALAKE_UI_TOKEN` set, the dev server's page is opened at `/#token=<the same>`.
const api = new URL(process.env.METALAKE_UI_URL ?? "http://127.0.0.1:19100").origin;

export default defineConfig({
  plugins: [react(), tailwindcss()],
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
    },
  },
  test: {
    environment: "jsdom",
    // The end-to-end tests are Playwright's, under e2e/.
    include: ["src/**/*.test.{ts,tsx}"],
    setupFiles: ["src/test/setup.ts"],
  },
});
