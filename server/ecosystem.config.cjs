/**
 * PM2 Ecosystem Configuration
 * 
 * This configures the READ-ONLY API server.
 * Ingestion runs separately via scripts/ingest/ (manual, cron, or CI).
 * 
 * Usage:
 *   pm2 start ecosystem.config.cjs
 *   pm2 start ecosystem.config.cjs --env production
 *   pm2 logs duckdb-api
 *   pm2 monit
 *   pm2 restart duckdb-api
 *   pm2 stop duckdb-api
 *   pm2 delete duckdb-api
 * 
 * Auto-start on boot:
 *   pm2 startup
 *   pm2 save
 */

module.exports = {
  apps: [
    {
      name: 'duckdb-api',
      script: 'server.js',
      cwd: __dirname,
      
      // Interpreter settings
      node_args: '--max-old-space-size=2048',
      
      // Restart behavior
      autorestart: true,
      watch: false,
      max_restarts: 10,
      min_uptime: '10s',
      restart_delay: 5000,
      
      // Crash recovery
      exp_backoff_restart_delay: 1000,
      
      // Memory management - lower limit since no ingestion
      max_memory_restart: '1.5G',
      
      // Logging
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      error_file: './logs/pm2-error.log',
      out_file: './logs/pm2-out.log',
      merge_logs: true,
      
      // Environment variables (defaults)
      env: {
        NODE_ENV: 'development',
        PORT: 3001,
      },
      
      // Production environment
      env_production: {
        NODE_ENV: 'production',
        PORT: 3001,
      },
    },
  ],
};
