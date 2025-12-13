import { spawn } from "child_process";
import path from "path";
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
  "infer_stage.py"
);

/**
 * Run stage inference via Python.
 */
export function inferStage(subject, content = "") {
  return new Promise((resolve) => {
    const proc = spawn("python", [SCRIPT_PATH]);

    let stdout = "";
    let stderr = "";

    proc.stdout.on("data", (d) => (stdout += d.toString()));
    proc.stderr.on("data", (d) => (stderr += d.toString()));

    proc.on("close", () => {
  try {
    const parsed = JSON.parse(stdout.trim());

    if (
      parsed &&
      ALLOWED_STAGES.has(parsed.stage) &&
      typeof parsed.confidence === "number"
    ) {
      return resolve({
        stage: parsed.stage,
        confidence: parsed.confidence,
      });
    }
  } catch (err) {
    console.error("[inferStage] JSON parse failed");
    console.error("stdout:", stdout.trim());
    console.error("stderr:", stderr.trim());
  }

  resolve(null);
});

    const text = `${subject}\n\n${content}`.slice(0, 800);
    proc.stdin.write(text);
    proc.stdin.end();
  });
}
