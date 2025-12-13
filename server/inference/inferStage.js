import { spawn } from "child_process";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

// Resolve __dirname (ESM-safe)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Allowed output labels
const ALLOWED_STAGES = new Set([
  "cip-discuss",
  "cip-vote",
  "cip-announce",
  "tokenomics",
  "tokenomics-announce",
  "sv-announce",
  "other",
]);

// Absolute path to Python script
const SCRIPT_PATH = path.resolve(
  __dirname,
  "..",
  "..",
  "scripts",
  "ingest",
  "infer_stage.py"
);

/**
 * Run stage inference via Python.
 */
export function inferStage(subject, content = "") {
  return new Promise((resolve) => {
    console.log(`[inferStage] Starting inference for: "${subject.slice(0, 50)}..."`);
    console.log(`[inferStage] Python script path: ${SCRIPT_PATH}`);
    
    // Check if script exists
    if (!fs.existsSync(SCRIPT_PATH)) {
      console.error(`[inferStage] Script not found at: ${SCRIPT_PATH}`);
      return resolve(null);
    }
    
    // Try python3 first (common on Linux/WSL), then python
    const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';
    console.log(`[inferStage] Using command: ${pythonCmd}`);
    
    const proc = spawn(pythonCmd, [SCRIPT_PATH]);

    let stdout = "";
    let stderr = "";

    proc.stdout.on("data", (d) => (stdout += d.toString()));
    proc.stderr.on("data", (d) => (stderr += d.toString()));

    proc.on("error", (err) => {
      console.error(`[inferStage] Failed to spawn process:`, err.message);
      resolve(null);
    });

    proc.on("close", (code) => {
      console.log(`[inferStage] Process exited with code: ${code}`);
      
      if (stderr.trim()) {
        console.log(`[inferStage] stderr: ${stderr.slice(0, 500)}`);
      }
      
      try {
        const parsed = JSON.parse(stdout.trim());

        if (
          parsed &&
          ALLOWED_STAGES.has(parsed.stage) &&
          typeof parsed.confidence === "number"
        ) {
          console.log(`[inferStage] Result: ${parsed.stage} (${parsed.confidence.toFixed(2)})`);
          return resolve({
            stage: parsed.stage,
            confidence: parsed.confidence,
          });
        }
      } catch (err) {
        console.error("[inferStage] JSON parse failed");
        console.error("stdout:", stdout.trim().slice(0, 200));
      }

      resolve(null);
    });

    const text = `${subject}\n\n${content}`.slice(0, 800);
    proc.stdin.write(text);
    proc.stdin.end();
  });
}
