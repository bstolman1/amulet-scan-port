module.exports = {
  apps: [
    {
      name: "duckdb-api",
      script: "server/server.js",   // ✅ FIXED
      cwd: "/home/ben/amulet-scan-port",
      env: {
        NODE_ENV: "production",
      },
    },
  ],
};

