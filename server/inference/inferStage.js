/**
 * Node.js wrapper for Python governance stage inference
 * 
 * Spawns Python process with isolation, passes input via stdin,
 * reads JSON output from stdout.
 * 
 * @module inference/inferStage
 */

import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PYTHON_SCRIPT = path.resolve(__dirname, '../../scripts/ingest/infer_stage.py');

// Default Python executable - can be overridden via env
const PYTHON_PATH = process.env.PYTHON_PATH || 'python3';

// Timeout for inference (model loading can take time on first run)
const INFERENCE_TIMEOUT_MS = 120000; // 2 minutes

/**
 * Run stage inference on a governance message
 * 
 * @param {string} subject - Message subject line
 * @param {string} [content] - Optional message content/body
 * @returns {Promise<{stage: string, confidence: number} | null>}
 */
export async function inferStage(subject, content = '') {
  const inputText = [subject, content].filter(Boolean).join('\n\n');
  
  if (!inputText.trim()) {
    return { stage: 'other', confidence: 0.0 };
  }
  
  return new Promise((resolve) => {
    const python = spawn(PYTHON_PATH, [PYTHON_SCRIPT], {
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: INFERENCE_TIMEOUT_MS,
    });
    
    let stdout = '';
    let stderr = '';
    
    python.stdout.on('data', (data) => {
      stdout += data.toString();
    });
    
    python.stderr.on('data', (data) => {
      stderr += data.toString();
    });
    
    python.on('close', (code) => {
      // Log stderr (non-fatal, just for debugging)
      if (stderr.trim()) {
        console.log('[inference]', stderr.trim());
      }
      
      if (code !== 0) {
        console.error(`[inference] Python exited with code ${code}`);
        resolve(null);
        return;
      }
      
      // Parse JSON output
      try {
        const result = JSON.parse(stdout.trim());
        
        // Validate result structure
        if (typeof result.stage !== 'string' || typeof result.confidence !== 'number') {
          console.error('[inference] Invalid result structure:', result);
          resolve(null);
          return;
        }
        
        resolve(result);
      } catch (err) {
        console.error('[inference] Failed to parse JSON:', stdout);
        resolve(null);
      }
    });
    
    python.on('error', (err) => {
      console.error('[inference] Failed to spawn Python:', err.message);
      resolve(null);
    });
    
    // Write input and close stdin
    python.stdin.write(inputText);
    python.stdin.end();
  });
}

/**
 * Batch inference for multiple messages
 * Runs sequentially to avoid memory issues with model loading
 * 
 * @param {Array<{subject: string, content?: string}>} messages
 * @returns {Promise<Array<{stage: string, confidence: number} | null>>}
 */
export async function inferStageBatch(messages) {
  const results = [];
  
  for (const msg of messages) {
    const result = await inferStage(msg.subject, msg.content);
    results.push(result);
  }
  
  return results;
}

export default inferStage;
