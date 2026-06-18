import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5173,
    proxy: {
      // Dev-mode parity with the nginx prod proxy.
      "/ws": {
        target: "ws://localhost:8765",
        ws: true,
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/ws/, ""),
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});
