// pm2 config for the standalone shared server (see server.cjs).
//   pm2 start ecosystem.config.cjs
//   pm2 save && pm2 startup    # survive reboots
// cwd is pinned to the repo root: data/*.json, the SQLite db, chat-history.json
// and the git-hash lookup all resolve relative to it.
module.exports = {
  apps: [
    {
      name: 'banks-dashboard',
      script: 'server.cjs',
      cwd: __dirname,
      instances: 1, // single instance: the shared in-memory cache + NS queue live here
      autorestart: true,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production',
        PORT: 8790,
        CACHE_TTL_MIN: 20,
        WARM_INTERVAL_MIN: 15,
      },
    },
  ],
};
