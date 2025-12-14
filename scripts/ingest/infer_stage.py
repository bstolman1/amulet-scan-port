#!/usr/bin/env python3
"""
Deterministic Local Governance Classification Engine

Zero-shot NLI-based classification of governance messages into fixed lifecycle stages.
Uses facebook/bart-large-mnli for deterministic, non-generative classification.

Input: Plain text via stdin (subject + optional content)
Output: JSON to stdout with {stage, confidence}

All logs go to stderr. Output is machine-parsable JSON only.
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
    print(msg, file=sys.stderr)

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
        print(json.dumps({"stage": "other", "confidence": 0.0}))
        sys.exit(1)
    
    log("Model loaded. Reading input from stdin...")
    
    # Read full text from stdin
    text = sys.stdin.read().strip()
    
    if not text:
        log("Empty input received")
        print(json.dumps({"stage": "other", "confidence": 0.0}))
        sys.exit(0)
    
    log(f"Classifying text ({len(text)} chars): {text[:100]}...")
    
    # Create candidate labels with descriptions for better matching
    candidate_labels = [STAGE_DESCRIPTIONS[stage] for stage in ALLOWED_STAGES]
    
    try:
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
        
        log(f"Classification result: {best_stage} (confidence: {best_score:.4f})")
        
        # Output strict JSON schema
        output = {
            "stage": best_stage,
            "confidence": round(best_score, 4)
        }
        print(json.dumps(output))
        
    except Exception as e:
        log(f"Classification error: {e}")
        print(json.dumps({"stage": "other", "confidence": 0.0}))
        sys.exit(1)

if __name__ == "__main__":
    main()
