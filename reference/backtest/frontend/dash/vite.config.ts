import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ command }) => {
  const apiTarget = process.env.VITE_API_PROXY_TARGET || 'http://localhost:8000';
  const isBuild = command === 'build';

  return {
    plugins: [react()],
    base: isBuild ? '/static/dash/' : '/',
    server: {
      host: true,
      port: 5173,
      strictPort: true,
      // Optional: set `VITE_USE_POLLING=1` for Docker Desktop/macOS file watching issues.
      watch: process.env.VITE_USE_POLLING === '1' ? { usePolling: true, interval: 125 } : undefined,
      proxy: {
        '/chart': apiTarget,
        '/ingest': apiTarget,
        '/backtests': apiTarget,
        '/symbols': apiTarget,
        '/docs': apiTarget,
        '/openapi.json': apiTarget,
      },
    },
    build: {
      outDir: 'dist',
      emptyOutDir: true,
    },
  };
});
