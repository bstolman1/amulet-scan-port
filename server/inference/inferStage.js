/**
 * Node.js Boundary Wrapper for Python Inference
 * 
 * Spawns Python process for zero-shot NLI classification.
 * Process isolation ensures determinism and clean error handling.
 */

import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PYTHON_SCRIPT = path.resolve(__dirname, '../../scripts/ingest/infer_stage.py');

// Python executable - try venv first, fall back to system python
const PYTHON_VENV = path.resolve(__dirname, '../../scripts/ingest/venv/bin/python');
const PYTHON_EXECUTABLE = process.env.INFERENCE_PYTHON || 'python3';

/**
 * Run inference on a governance message
 * 
 * @param {string} subject - Topic subject line (required)
 * @param {string} [content] - Topic content/snippet (optional)
 * @returns {Promise<{stage: string, confidence: number} | null>} - Classification result or null on error
 */
export async function inferStage(subject, content = '') {
  const inputText = `${subject}\n${content}`.trim();
  
  if (!inputText) {
    console.warn('[inferStage] Empty input, returning null');
    return null;
  }
  
  return new Promise((resolve) => {
    let stdout = '';
    let stderr = '';
    let resolved = false;
    
    // Determine which Python to use
    const pythonCmd = process.env.INFERENCE_PYTHON || PYTHON_EXECUTABLE;
    
    const proc = spawn(pythonCmd, [PYTHON_SCRIPT], {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env },
    });
    
    // Set a timeout to prevent hanging
    const timeout = setTimeout(() => {
      if (!resolved) {
        resolved = true;
        proc.kill('SIGKILL');
        console.error('[inferStage] Process timeout after 60s');
        resolve(null);
      }
    }, 60000);
    
    proc.stdout.on('data', (data) => {
      stdout += data.toString();
    });
    
    proc.stderr.on('data', (data) => {
      stderr += data.toString();
    });
    
    proc.on('close', (code) => {
      clearTimeout(timeout);
      if (resolved) return;
      resolved = true;
      
      // Log stderr (non-fatal, just informational)
      if (stderr.trim()) {
        console.log('[inferStage]', stderr.trim());
      }
      
      if (code !== 0) {
        console.error(`[inferStage] Process exited with code ${code}`);
        resolve(null);
        return;
      }
      
      // Parse JSON output
      try {
        const result = JSON.parse(stdout.trim());
        
        // Validate schema
        if (typeof result.stage !== 'string' || typeof result.confidence !== 'number') {
          console.error('[inferStage] Invalid response schema:', result);
          resolve(null);
          return;
        }
        
        resolve(result);
      } catch (parseErr) {
        console.error('[inferStage] Failed to parse JSON output:', stdout);
        resolve(null);
      }
    });
    
    proc.on('error', (err) => {
      clearTimeout(timeout);
      if (!resolved) {
        resolved = true;
        console.error('[inferStage] Failed to spawn process:', err.message);
        resolve(null);
      }
    });
    
    // Write input and close stdin
    proc.stdin.write(inputText);
    proc.stdin.end();
  });
}

/**
 * Batch inference for multiple topics
 * Runs sequentially to maintain determinism and avoid resource contention
 * 
 * @param {Array<{subject: string, content?: string}>} topics
 * @returns {Promise<Array<{stage: string, confidence: number} | null>>}
 */
export async function inferStagesBatch(topics) {
  const results = [];
  for (const topic of topics) {
    const result = await inferStage(topic.subject, topic.content);
    results.push(result);
  }
  return results;
}

export default inferStage;
