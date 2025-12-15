#!/usr/bin/env python3
"""
Deterministic Local Governance Classification Engine

Zero-shot NLI-based classification of governance messages into fixed lifecycle stages.
Uses facebook/bart-large-mnli for deterministic, non-generative classification.

Input: JSONL via stdin - each line is {"id": "...", "text": "..."}
Output: JSONL to stdout - each line is {"id": "...", "stage": "...", "confidence": 0.XX}

All logs go to stderr. Output is machine-parsable JSONL only.
"""

import sys
import json
from transformers import pipeline

# Fixed set of allowed stages - model can ONLY choose from these
ALLOWED_STAGES = [
    "cip-discuss",
    "cip-vote", 
    "cip-announce",
    "tokenomics",
    "tokenomics-announce",
    "sv-announce",
    "other"
]

# Human-readable hypothesis templates for each stage
STAGE_DESCRIPTIONS = {
    "cip-discuss": "a Canton Improvement Proposal discussion",
    "cip-vote": "a Canton Improvement Proposal vote",
    "cip-announce": "a Canton Improvement Proposal announcement",
    "tokenomics": "a tokenomics discussion about featured apps or validators",
    "tokenomics-announce": "a tokenomics announcement for featured apps",
    "sv-announce": "a Super Validator announcement or approval",
    "other": "a general governance topic"
}

def log(msg):
    """Log to stderr only - stdout is reserved for JSON output"""
    print(msg, file=sys.stderr, flush=True)

def main():
    log("Loading NLI classification model...")
    
    # Load zero-shot classification pipeline
    # Using BART-large-MNLI for deterministic classification
    try:
        classifier = pipeline(
            "zero-shot-classification",
            model="facebook/bart-large-mnli",
            device=-1  # CPU for determinism
        )
    except Exception as e:
        log(f"Error loading model: {e}")
        sys.exit(1)
    
    log("Model loaded. Processing JSONL input from stdin...")
    
    # Create candidate labels with descriptions for better matching
    candidate_labels = [STAGE_DESCRIPTIONS[stage] for stage in ALLOWED_STAGES]
    
    # Process each line as a JSON object
    processed = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            item = json.loads(line)
            item_id = item.get("id", "unknown")
            text = item.get("text", "").strip()
            
            if not text:
                # Empty text - return default
                print(json.dumps({"id": item_id, "stage": "other", "confidence": 0.0}), flush=True)
                continue
            
            # Run zero-shot classification
            result = classifier(
                text,
                candidate_labels,
                hypothesis_template="This message is about {}.",
                multi_label=False  # Single best label only
            )
            
            # Map back from description to stage label
            best_description = result["labels"][0]
            best_score = result["scores"][0]
            
            # Find the stage that matches this description
            best_stage = "other"
            for stage, desc in STAGE_DESCRIPTIONS.items():
                if desc == best_description:
                    best_stage = stage
                    break
            
            # Output strict JSON schema
            output = {
                "id": item_id,
                "stage": best_stage,
                "confidence": round(best_score, 4)
            }
            print(json.dumps(output), flush=True)
            
            processed += 1
            if processed % 50 == 0:
                log(f"Processed {processed} items...")
                
        except json.JSONDecodeError as e:
            log(f"Invalid JSON line: {e}")
            continue
        except Exception as e:
            log(f"Classification error: {e}")
            # Output error result for this item
            try:
                item_id = json.loads(line).get("id", "unknown")
                print(json.dumps({"id": item_id, "stage": "other", "confidence": 0.0}), flush=True)
            except:
                pass
            continue
    
    log(f"Inference complete. Processed {processed} items total.")

if __name__ == "__main__":
    main()
