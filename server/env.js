// Load environment variables BEFORE any other imports
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const envPath = path.join(__dirname, '.env');
console.log('📁 Loading .env from:', envPath);
// Important: npm scripts may also pass --env-file; we want server/.env to win.
const result = dotenv.config({ path: envPath, override: true });
if (result.error) {
  console.error('❌ Failed to load .env:', result.error.message);
}

console.log('📁 DATA_DIR:', process.env.DATA_DIR || 'NOT SET');
console.log('📁 CURSOR_DIR:', process.env.CURSOR_DIR || 'NOT SET');
console.log('🔑 GROUPS_IO_API_KEY loaded:', process.env.GROUPS_IO_API_KEY ? 'YES' : 'NO');
