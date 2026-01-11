// Load environment variables BEFORE any other imports
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const envPath = path.join(__dirname, '.env');

// Load .env silently - avoid logging paths or variable names for security
const result = dotenv.config({ path: envPath });
if (result.error) {
  // Only log generic error, not the specific path or variable names
  console.error('❌ Failed to load environment configuration');
}

// Validate required variables exist without logging their names
const requiredVars = ['DATA_DIR'];
const missingVars = requiredVars.filter(v => !process.env[v]);
if (missingVars.length > 0) {
  console.warn('⚠️ Some environment variables are not set - using defaults');
}
