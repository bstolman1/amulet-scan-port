// Load environment variables BEFORE any other imports
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '.env') });

console.log('🔑 GROUPS_IO_API_KEY loaded:', process.env.GROUPS_IO_API_KEY ? 'YES (length: ' + process.env.GROUPS_IO_API_KEY.length + ')' : 'NO');
