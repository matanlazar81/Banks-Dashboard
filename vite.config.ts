import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'
import { execSync } from 'child_process'

// Every /api/* handler lives in server/api-routes.cjs, shared verbatim between this dev
// server and the standalone production server (server.cjs → `npm run serve`). Keep all
// route logic there; this plugin only mounts it onto Vite's connect app.
function banksPlugin(): Plugin {
  return {
    name: 'banks-api',
    configureServer(server) {
      const modPath = path.resolve(__dirname, 'server', 'api-routes.cjs');
      delete require.cache[require.resolve(modPath)]; // fresh copy on every (re)start
      require(modPath).registerApiRoutes(server.middlewares);
      // Edit-and-reload workflow: a change to the routes module restarts the dev server
      // (mirrors the old behavior of editing the handlers inside vite.config.ts).
      server.watcher.add(modPath);
      server.watcher.on('change', (p) => {
        if (path.resolve(p) === modPath) {
          server.config.logger.info('[banks-api] server/api-routes.cjs changed — restarting dev server');
          server.restart();
        }
      });
    },
  };
}

const gitHash = (() => { try { return execSync('git rev-parse --short HEAD', { cwd: __dirname }).toString().trim(); } catch { return 'unknown'; } })();

export default defineConfig({
  root: path.resolve(__dirname),
  plugins: [react(), tailwindcss(), banksPlugin()],
  define: { '__GIT_HASH__': JSON.stringify(gitHash) },
  resolve: { alias: { 'xlsx': 'xlsx-js-style' } },
  build: { chunkSizeWarningLimit: 2000 },
  server: { port: 5176 },
})
