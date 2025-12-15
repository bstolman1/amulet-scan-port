/**
 * Node.js Boundary Wrapper for Python Inference
 * 
 * Spawns Python process for zero-shot NLI classification.
 * Uses BATCH processing - loads model once, processes all topics via JSONL stream.
 */

import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import { createInterface } from 'readline';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PYTHON_SCRIPT = path.resolve(__dirname, '../../scripts/ingest/infer_stage.py');

// Python executable - configurable via env
const PYTHON_EXECUTABLE = process.env.INFERENCE_PYTHON || 'python3';

/**
 * Run batch inference on multiple governance messages
 * 
 * This loads the model ONCE and processes all topics in a single process.
 * Much faster than spawning a new process per topic.
 * 
 * @param {Array<{id: string, subject: string, content?: string}>} topics
 * @param {function} onProgress - Optional callback for progress updates
 * @returns {Promise<Map<string, {stage: string, confidence: number}>>} - Map of id -> result
 */
export async function inferStagesBatch(topics, onProgress = null) {
  if (!topics || topics.length === 0) {
    return new Map();
  }
  
  console.log(`[inferStage] Starting batch inference for ${topics.length} topics...`);
  
  return new Promise((resolve, reject) => {
    const results = new Map();
    let processedCount = 0;
    
    const proc = spawn(PYTHON_EXECUTABLE, [PYTHON_SCRIPT], {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env },
    });
    
    // Set a generous timeout for batch processing (5 min for model load + processing)
    const timeout = setTimeout(() => {
      proc.kill('SIGKILL');
      console.error(`[inferStage] Batch process timeout after 5 minutes`);
      resolve(results); // Return partial results
    }, 300000);
    
    // Read results line by line as they stream back
    const rl = createInterface({ input: proc.stdout });
    
    rl.on('line', (line) => {
      try {
        const result = JSON.parse(line);
        if (result.id) {
          results.set(result.id, {
            stage: result.stage,
            confidence: result.confidence,
          });
          processedCount++;
          
          if (onProgress && processedCount % 50 === 0) {
            onProgress(processedCount, topics.length);
          }
        }
      } catch (e) {
        // Skip invalid JSON lines
      }
    });
    
    // Log stderr (model loading progress, etc)
    proc.stderr.on('data', (data) => {
      const msg = data.toString().trim();
      if (msg) {
        console.log('[inferStage]', msg);
      }
    });
    
    proc.on('close', (code) => {
      clearTimeout(timeout);
      
      if (code !== 0) {
        console.error(`[inferStage] Process exited with code ${code}`);
      }
      
      console.log(`[inferStage] Batch complete: ${results.size}/${topics.length} classified`);
      resolve(results);
    });
    
    proc.on('error', (err) => {
      clearTimeout(timeout);
      console.error('[inferStage] Failed to spawn process:', err.message);
      resolve(results); // Return empty/partial results
    });
    
    // Write all topics as JSONL to stdin
    for (const topic of topics) {
      const text = `${topic.subject}\n${topic.content || ''}`.trim();
      const jsonLine = JSON.stringify({ id: topic.id, text }) + '\n';
      proc.stdin.write(jsonLine);
    }
    
    // Close stdin to signal end of input
    proc.stdin.end();
  });
}

/**
 * Single topic inference - convenience wrapper around batch
 * 
 * @param {string} subject - Topic subject line
 * @param {string} [content] - Topic content/snippet
 * @returns {Promise<{stage: string, confidence: number} | null>}
 */
export async function inferStage(subject, content = '') {
  const results = await inferStagesBatch([
    { id: 'single', subject, content }
  ]);
  
  return results.get('single') || null;
}

export default inferStage;
