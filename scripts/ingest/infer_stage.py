#!/usr/bin/env python3
"""
Deterministic Zero-Shot Governance Stage Classification

Uses NLI-based zero-shot classification to assign governance messages
to a fixed set of lifecycle stages.

Input: Raw text via stdin (subject + content)
Output: JSON to stdout: {"stage": "<label>", "confidence": <float>}

All logs go to stderr. No text generation. No hallucination.
"""

import sys
import json

# Suppress all library logging to stderr only
import logging
logging.basicConfig(level=logging.ERROR, stream=sys.stderr)

# Fixed label set - model CANNOT output anything else
LABELS = [
    "cip-discuss",
    "cip-vote", 
    "cip-announce",
    "tokenomics",
    "tokenomics-announce",
    "sv-announce",
    "other"
]

# Hypothesis template for NLI classification
HYPOTHESIS_TEMPLATE = "This message is about {}."

# Human-readable label descriptions for better classification
LABEL_DESCRIPTIONS = {
    "cip-discuss": "CIP discussion or canton improvement proposal discussion",
    "cip-vote": "CIP voting or canton improvement proposal vote",
    "cip-announce": "CIP announcement or canton improvement proposal announcement",
    "tokenomics": "tokenomics discussion or featured app tokenomics or validator application",
    "tokenomics-announce": "tokenomics announcement or featured app approval announcement",
    "sv-announce": "super validator announcement or final approval announcement",
    "other": "general discussion or unrelated topic"
}


def load_model():
    """Load the zero-shot classification pipeline once."""
    print("Loading zero-shot classification model...", file=sys.stderr)
    
    from transformers import pipeline
    import torch
    
    # Use CPU for determinism
    device = 0 if torch.cuda.is_available() else -1
    
    classifier = pipeline(
        "zero-shot-classification",
        model="facebook/bart-large-mnli",
        device=device
    )
    
    print("Model loaded successfully", file=sys.stderr)
    return classifier


def classify(classifier, text):
    """
    Classify text into one of the fixed governance stages.
    
    Returns: dict with 'stage' and 'confidence'
    """
    if not text or not text.strip():
        return {"stage": "other", "confidence": 0.0}
    
    # Use descriptive labels for better classification accuracy
    candidate_labels = [LABEL_DESCRIPTIONS[label] for label in LABELS]
    
    result = classifier(
        text,
        candidate_labels,
        hypothesis_template=HYPOTHESIS_TEMPLATE,
        multi_label=False
    )
    
    # Map back to original label
    best_description = result["labels"][0]
    best_score = result["scores"][0]
    
    # Find the original label for this description
    best_label = "other"
    for label, desc in LABEL_DESCRIPTIONS.items():
        if desc == best_description:
            best_label = label
            break
    
    return {
        "stage": best_label,
        "confidence": round(best_score, 4)
    }


def main():
    """Main entry point - reads stdin, outputs JSON to stdout."""
    # Read all input
    text = sys.stdin.read().strip()
    
    if not text:
        # Empty input - output default
        print(json.dumps({"stage": "other", "confidence": 0.0}))
        return
    
    try:
        classifier = load_model()
        result = classify(classifier, text)
        print(json.dumps(result))
    except Exception as e:
        print(f"Classification error: {e}", file=sys.stderr)
        # On error, output safe default
        print(json.dumps({"stage": "other", "confidence": 0.0}))
        sys.exit(1)


if __name__ == "__main__":
    main()
