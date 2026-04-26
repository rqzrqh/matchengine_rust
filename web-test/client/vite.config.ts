import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import yaml from "js-yaml";
import { defineConfig } from "vite";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const yamlPath = path.resolve(__dirname, "../../config.yaml");

const rootCfg = yaml.load(fs.readFileSync(yamlPath, "utf8")) as Record<string, unknown>;

/** When you run only Vite (e.g. port 5173), forward API/WebSocket to the web-test Express app (default :8000). */
const apiProxyTarget = process.env.WEB_TEST_API_PROXY_TARGET ?? "http://127.0.0.1:8000";

export default defineConfig({
  root: __dirname,
  define: {
    __MATCENGINE_CONFIG__: JSON.stringify(rootCfg),
  },
  server: {
    proxy: {
      "/api": { target: apiProxyTarget, changeOrigin: true },
      "/ws": { target: apiProxyTarget.replace(/^http/, "ws"), ws: true },
    },
  },
  build: {
    outDir: path.resolve(__dirname, "../dist/client"),
    emptyOutDir: true,
  },
});
