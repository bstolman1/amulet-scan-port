/**
 * PM2 Ecosystem Configuration
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
      
      // Interpreter settings - enable GC exposure for memory management
      node_args: '--expose-gc --max-old-space-size=4096',
      
      // Restart behavior
      autorestart: true,
      watch: false, // Don't use watch on Windows with DuckDB
      max_restarts: 10, // Max restarts within min_uptime window
      min_uptime: '10s', // Consider started if running for 10s
      restart_delay: 5000, // Wait 5s between restarts
      
      // Crash recovery
      exp_backoff_restart_delay: 1000, // Exponential backoff starting at 1s
      
      // Memory management - restart if memory exceeds threshold
      max_memory_restart: '3G',
      
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
    
    // Optional: Ingestion worker (uncomment to enable)
    // {
    //   name: 'ingest-updates',
    //   script: '../scripts/ingest/fetch-updates.js',
    //   cwd: __dirname,
    //   autorestart: true,
    //   watch: false,
    //   max_restarts: 5,
    //   restart_delay: 30000, // 30s between restarts
    //   max_memory_restart: '2G',
    //   log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    //   error_file: './logs/ingest-error.log',
    //   out_file: './logs/ingest-out.log',
    //   env: {
    //     NODE_ENV: 'production',
    //   },
    // },
  ],
};
