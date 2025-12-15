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
    import gc
    import os
    
    # Memory optimization settings
    os.environ.setdefault('PYTORCH_CUDA_ALLOC_CONF', 'max_split_size_mb:128')
    os.environ.setdefault('TRANSFORMERS_CACHE', '/tmp/hf_cache')
    
    log("Loading NLI classification model...")
    
    # Load zero-shot classification pipeline
    # Using DistilBERT-MNLI for memory efficiency (~250MB vs ~1.6GB)
    try:
        classifier = pipeline(
            "zero-shot-classification",
            model="typeform/distilbert-base-uncased-mnli",
            device=-1,  # CPU for determinism
        )
    except Exception as e:
        log(f"Error loading model: {e}")
        sys.exit(1)
    
    log("Model loaded. Processing JSONL input from stdin...")
    
    # Create candidate labels with descriptions for better matching
    candidate_labels = [STAGE_DESCRIPTIONS[stage] for stage in ALLOWED_STAGES]
    
    # Buffer items and process in mini-batches for memory efficiency
    BATCH_SIZE = 10  # Process 10 at a time, then GC
    buffer = []
    processed = 0
    
    def process_batch(items):
        """Process a mini-batch and free memory"""
        nonlocal processed
        for item in items:
            item_id = item.get("id", "unknown")
            text = item.get("text", "").strip()
            
            if not text:
                print(json.dumps({"id": item_id, "stage": "other", "confidence": 0.0}), flush=True)
                processed += 1
                continue
            
            try:
                # Run zero-shot classification
                result = classifier(
                    text,
                    candidate_labels,
                    hypothesis_template="This message is about {}.",
                    multi_label=False
                )
                
                # Map back from description to stage label
                best_description = result["labels"][0]
                best_score = result["scores"][0]
                
                best_stage = "other"
                for stage, desc in STAGE_DESCRIPTIONS.items():
                    if desc == best_description:
                        best_stage = stage
                        break
                
                print(json.dumps({
                    "id": item_id,
                    "stage": best_stage,
                    "confidence": round(best_score, 4)
                }), flush=True)
                
            except Exception as e:
                log(f"Classification error for {item_id}: {e}")
                print(json.dumps({"id": item_id, "stage": "other", "confidence": 0.0}), flush=True)
            
            processed += 1
            if processed % 50 == 0:
                log(f"Processed {processed} items...")
        
        # Force garbage collection after each batch
        gc.collect()
    
    # Read and buffer items
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            item = json.loads(line)
            buffer.append(item)
            
            # Process when buffer is full
            if len(buffer) >= BATCH_SIZE:
                process_batch(buffer)
                buffer = []
                
        except json.JSONDecodeError as e:
            log(f"Invalid JSON line: {e}")
            continue
    
    # Process remaining items
    if buffer:
        process_batch(buffer)
    
    log(f"Inference complete. Processed {processed} items total.")

if __name__ == "__main__":
    main()
