// Load environment variables BEFORE any other imports
import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const envPath = path.join(__dirname, '.env');

// Capture any pre-existing env values (possibly from --env-file)
const preExisting = {
  DATA_DIR: process.env.DATA_DIR,
  CURSOR_DIR: process.env.CURSOR_DIR,
  ENGINE_ENABLED: process.env.ENGINE_ENABLED,
};

console.log('📁 Loading .env from:', envPath);

// Important: npm scripts may also pass --env-file; we want server/.env to win.
const result = dotenv.config({ path: envPath, override: true });
if (result.error) {
  console.error('❌ Failed to load .env:', result.error.message);
}

// Detect and warn about conflicts
const criticalKeys = ['DATA_DIR', 'CURSOR_DIR', 'ENGINE_ENABLED'];
const conflicts = [];
for (const key of criticalKeys) {
  const before = preExisting[key];
  const after = process.env[key];
  if (before && after && before !== after) {
    conflicts.push({ key, cliValue: before, envFileValue: after });
  }
}

if (conflicts.length > 0) {
  console.warn('⚠️  ENV CONFLICT DETECTED: --env-file values were overridden by server/.env');
  for (const c of conflicts) {
    console.warn(`   ${c.key}: CLI="${c.cliValue}" → .env="${c.envFileValue}" (using .env)`);
  }
  console.warn('   TIP: Remove --env-file from npm scripts to avoid this warning.');
}

console.log('📁 DATA_DIR:', process.env.DATA_DIR || 'NOT SET');
console.log('📁 CURSOR_DIR:', process.env.CURSOR_DIR || 'NOT SET');
console.log('🔑 GROUPS_IO_API_KEY loaded:', process.env.GROUPS_IO_API_KEY ? 'YES' : 'NO');
