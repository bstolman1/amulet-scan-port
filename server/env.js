// Load environment variables BEFORE any other imports
import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';

// This project can be started from either repo root or /server.
// Prefer an .env next to the running process, then fall back to /server/.env from repo root.
const cwd = process.cwd();
const candidatePaths = [
  path.join(cwd, '.env'),
  path.join(cwd, 'server', '.env'),
];

const envPath = candidatePaths.find((p) => fs.existsSync(p));

// Load .env silently - avoid logging paths or variable names for security
const result = envPath ? dotenv.config({ path: envPath }) : { error: new Error('env file not found') };
if (result.error) {
  // Only log generic error, not the specific path or variable names
  console.error('❌ Failed to load environment configuration');
}

// Validate required variables exist without logging their names
const requiredVars = ['DATA_DIR'];
const missingVars = requiredVars.filter((v) => !process.env[v]);
if (missingVars.length > 0) {
  console.warn('⚠️ Some environment variables are not set - using defaults');
}
