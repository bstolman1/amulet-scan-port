/**
 * PM2 Staging Ecosystem Configuration
 *
 * Runs a separate backend instance on port 3002 for staging.
 * Independent of the production backend (port 3001).
 *
 * Usage:
 *   pm2 start ecosystem.staging.cjs
 *   pm2 logs duckdb-api-staging
 *   pm2 restart duckdb-api-staging
 *   pm2 stop duckdb-api-staging
 */

module.exports = {
  apps: [
    {
      name: 'duckdb-api-staging',
      script: 'server.js',
      cwd: __dirname,

      // Interpreter settings
      node_args: '--max-old-space-size=2048 --env-file=../scripts/ingest/.env',

      // Restart behavior
      autorestart: true,
      watch: false,
      max_restarts: 10,
      min_uptime: '10s',
      restart_delay: 5000,

      // Crash recovery
      exp_backoff_restart_delay: 1000,

      // Memory management
      max_memory_restart: '1024M',

      // Logging
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: './logs/pm2-staging-error.log',
      out_file: './logs/pm2-staging-out.log',
      merge_logs: true,

      // Environment variables
      env: {
        NODE_ENV: 'staging',
        PORT: 3002,
      },
    },
  ],
};
