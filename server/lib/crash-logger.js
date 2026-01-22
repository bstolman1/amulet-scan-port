/**
 * Crash Logger - writes uncaught exceptions and unhandled rejections to disk
 * Log files are stored in the server/logs directory
 */
import fs from 'fs';
import path from 'path';

// Log directory - relative to server folder
const LOG_DIR = process.env.LOG_DIR || path.join(process.cwd(), 'server', 'logs');
const CRASH_LOG_FILE = path.join(LOG_DIR, 'crash.log');
const ERROR_LOG_FILE = path.join(LOG_DIR, 'error.log');

// Ensure log directory exists
function ensureLogDir() {
  if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR, { recursive: true });
  }
}

// Format error for logging
function formatError(error, type) {
  const timestamp = new Date().toISOString();
  const separator = '='.repeat(80);
  
  return `
${separator}
[${timestamp}] ${type}
${separator}
Message: ${error?.message || 'Unknown error'}
Stack: ${error?.stack || 'No stack trace'}
Process PID: ${process.pid}
Node Version: ${process.version}
Platform: ${process.platform}
Memory Usage: ${JSON.stringify(process.memoryUsage(), null, 2)}
Uptime: ${process.uptime().toFixed(2)} seconds
${separator}

`;
}

// Append to log file
function appendToLog(filePath, content) {
  try {
    ensureLogDir();
    fs.appendFileSync(filePath, content, 'utf8');
  } catch (err) {
    // Last resort - write to stderr
    process.stderr.write(`Failed to write to ${filePath}: ${err.message}\n`);
    process.stderr.write(content);
  }
}

// Log crash to file
export function logCrash(error, type = 'UNCAUGHT_EXCEPTION') {
  const formatted = formatError(error, type);
  appendToLog(CRASH_LOG_FILE, formatted);
  console.error(`💥 ${type} logged to ${CRASH_LOG_FILE}`);
}

// Log error to file (for non-fatal errors you want to persist)
export function logError(error, context = 'ERROR') {
  const formatted = formatError(error, context);
  appendToLog(ERROR_LOG_FILE, formatted);
}

// Install global crash handlers
export function installCrashHandlers() {
  ensureLogDir();
  
  // Uncaught exceptions
  process.on('uncaughtException', (error) => {
    logCrash(error, 'UNCAUGHT_EXCEPTION');
    console.error('💥 Uncaught Exception:', error.message);
    console.error(error.stack);
    // Give time to write logs before exiting
    setTimeout(() => process.exit(1), 1000);
  });

  // Unhandled promise rejections
  process.on('unhandledRejection', (reason, promise) => {
    const error = reason instanceof Error ? reason : new Error(String(reason));
    logCrash(error, 'UNHANDLED_REJECTION');
    console.error('💥 Unhandled Rejection:', error.message);
  });

  // SIGTERM/SIGINT - graceful shutdown logging
  ['SIGTERM', 'SIGINT'].forEach(signal => {
    process.on(signal, () => {
      const shutdownLog = `[${new Date().toISOString()}] ${signal} received - graceful shutdown\n`;
      appendToLog(CRASH_LOG_FILE, shutdownLog);
      console.log(`\n🛑 ${signal} received - shutting down gracefully...`);
      process.exit(0);
    });
  });

  console.log(`📝 Crash logger installed - logs written to ${LOG_DIR}`);
}

// Export log paths for reference
export const LOG_PATHS = {
  dir: LOG_DIR,
  crash: CRASH_LOG_FILE,
  error: ERROR_LOG_FILE,
};

export default { installCrashHandlers, logCrash, logError, LOG_PATHS };
