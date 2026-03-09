/**
 * Crash Logger - writes uncaught exceptions and unhandled rejections to disk
 * Log files are stored in the server/logs directory
 */
// @ts-nocheck
function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
import fs from 'fs';
import path from 'path';

// Log directory - relative to server folder
const LOG_DIR = stryMutAct_9fa48("94") ? process.env.LOG_DIR && path.join(process.cwd(), 'server', 'logs') : stryMutAct_9fa48("93") ? false : stryMutAct_9fa48("92") ? true : (stryCov_9fa48("92", "93", "94"), process.env.LOG_DIR || path.join(process.cwd(), 'server', 'logs'));
const CRASH_LOG_FILE = path.join(LOG_DIR, 'crash.log');
const ERROR_LOG_FILE = path.join(LOG_DIR, 'error.log');

// Ensure log directory exists
function ensureLogDir() {
  if (stryMutAct_9fa48("99")) {
    {}
  } else {
    stryCov_9fa48("99");
    if (stryMutAct_9fa48("102") ? false : stryMutAct_9fa48("101") ? true : stryMutAct_9fa48("100") ? fs.existsSync(LOG_DIR) : (stryCov_9fa48("100", "101", "102"), !fs.existsSync(LOG_DIR))) {
      if (stryMutAct_9fa48("103")) {
        {}
      } else {
        stryCov_9fa48("103");
        fs.mkdirSync(LOG_DIR, {
          recursive: stryMutAct_9fa48("105") ? false : (stryCov_9fa48("105"), true)
        });
      }
    }
  }
}

// Format error for logging
function formatError(error, type) {
  if (stryMutAct_9fa48("106")) {
    {}
  } else {
    stryCov_9fa48("106");
    const timestamp = new Date().toISOString();
    const separator = '='.repeat(80);
    return `
${separator}
[${timestamp}] ${type}
${separator}
Message: ${stryMutAct_9fa48("111") ? error?.message && 'Unknown error' : stryMutAct_9fa48("110") ? false : stryMutAct_9fa48("109") ? true : (stryCov_9fa48("109", "110", "111"), (stryMutAct_9fa48("112") ? error.message : (stryCov_9fa48("112"), error?.message)) || 'Unknown error')}
Stack: ${stryMutAct_9fa48("116") ? error?.stack && 'No stack trace' : stryMutAct_9fa48("115") ? false : stryMutAct_9fa48("114") ? true : (stryCov_9fa48("114", "115", "116"), (stryMutAct_9fa48("117") ? error.stack : (stryCov_9fa48("117"), error?.stack)) || 'No stack trace')}
Process PID: ${process.pid}
Node Version: ${process.version}
Platform: ${process.platform}
Memory Usage: ${JSON.stringify(process.memoryUsage(), null, 2)}
Uptime: ${process.uptime().toFixed(2)} seconds
${separator}

`;
  }
}

// Append to log file
function appendToLog(filePath, content) {
  if (stryMutAct_9fa48("119")) {
    {}
  } else {
    stryCov_9fa48("119");
    try {
      if (stryMutAct_9fa48("120")) {
        {}
      } else {
        stryCov_9fa48("120");
        ensureLogDir();
        fs.appendFileSync(filePath, content, 'utf8');
      }
    } catch (err) {
      if (stryMutAct_9fa48("122")) {
        {}
      } else {
        stryCov_9fa48("122");
        // Last resort - write to stderr
        process.stderr.write(`Failed to write to ${filePath}: ${err.message}\n`);
        process.stderr.write(content);
      }
    }
  }
}

// Log crash to file
export function logCrash(error, type = 'UNCAUGHT_EXCEPTION') {
  if (stryMutAct_9fa48("125")) {
    {}
  } else {
    stryCov_9fa48("125");
    const formatted = formatError(error, type);
    appendToLog(CRASH_LOG_FILE, formatted);
    console.error(`💥 ${type} logged to ${CRASH_LOG_FILE}`);
  }
}

// Log error to file (for non-fatal errors you want to persist)
export function logError(error, context = 'ERROR') {
  if (stryMutAct_9fa48("128")) {
    {}
  } else {
    stryCov_9fa48("128");
    const formatted = formatError(error, context);
    appendToLog(ERROR_LOG_FILE, formatted);
  }
}

// Install global crash handlers
export function installCrashHandlers() {
  if (stryMutAct_9fa48("129")) {
    {}
  } else {
    stryCov_9fa48("129");
    ensureLogDir();

    // Uncaught exceptions
    process.on('uncaughtException', error => {
      if (stryMutAct_9fa48("131")) {
        {}
      } else {
        stryCov_9fa48("131");
        logCrash(error, 'UNCAUGHT_EXCEPTION');
        console.error('💥 Uncaught Exception:', error.message);
        console.error(error.stack);
        // Give time to write logs before exiting
        setTimeout(stryMutAct_9fa48("134") ? () => undefined : (stryCov_9fa48("134"), () => process.exit(1)), 1000);
      }
    });

    // Unhandled promise rejections
    process.on('unhandledRejection', (reason, promise) => {
      if (stryMutAct_9fa48("136")) {
        {}
      } else {
        stryCov_9fa48("136");
        const error = reason instanceof Error ? reason : new Error(String(reason));
        logCrash(error, 'UNHANDLED_REJECTION');
        console.error('💥 Unhandled Rejection:', error.message);
      }
    });

    // SIGTERM/SIGINT - graceful shutdown logging
    (stryMutAct_9fa48("139") ? [] : (stryCov_9fa48("139"), ['SIGTERM', 'SIGINT'])).forEach(signal => {
      if (stryMutAct_9fa48("142")) {
        {}
      } else {
        stryCov_9fa48("142");
        process.on(signal, () => {
          if (stryMutAct_9fa48("143")) {
            {}
          } else {
            stryCov_9fa48("143");
            const shutdownLog = `[${new Date().toISOString()}] ${signal} received - graceful shutdown\n`;
            appendToLog(CRASH_LOG_FILE, shutdownLog);
            console.log(`\n🛑 ${signal} received - shutting down gracefully...`);
            process.exit(0);
          }
        });
      }
    });
    console.log(`📝 Crash logger installed - logs written to ${LOG_DIR}`);
  }
}

// Export log paths for reference
export const LOG_PATHS = {
  dir: LOG_DIR,
  crash: CRASH_LOG_FILE,
  error: ERROR_LOG_FILE
};
export default {
  installCrashHandlers,
  logCrash,
  logError,
  LOG_PATHS
};