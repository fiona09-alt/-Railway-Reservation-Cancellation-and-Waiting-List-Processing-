import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Vite is started with `vite client`, so this config lives in client/ and the
// root defaults to this directory. Dev server proxies API + WebSocket to the
// Node backend that hosts the real multithreaded simulation.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:4000',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://localhost:4000',
        ws: true,
      },
    },
  },
  build: {
    outDir: '../server/public',
    emptyOutDir: true,
  },
});
