import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// In development, `/api` goes to a running `metalake ui --no-browser`, whose URL `METALAKE_UI_URL` names.
const api = process.env.METALAKE_UI_URL ?? "http://127.0.0.1:19100";

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
    proxy: { "/api": api },
  },
  test: {
    environment: "jsdom",
    // The end-to-end tests are Playwright's, under e2e/.
    include: ["src/**/*.test.{ts,tsx}"],
    setupFiles: ["src/test/setup.ts"],
  },
});
