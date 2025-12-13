# scripts/infer_stage.py
import sys
import json
from transformers import pipeline

LABELS = [
    "cip-discuss",
    "cip-vote",
    "cip-announce",
    "tokenomics",
    "tokenomics-announce",
    "sv-announce",
    "other"
]

classifier = pipeline(
    "zero-shot-classification",
    model="facebook/bart-large-mnli"
)

text = sys.stdin.read().strip()

if not text:
    print(json.dumps({
        "stage": "other",
        "confidence": 0.0
    }))
    sys.exit(0)

result = classifier(
    text,
    candidate_labels=LABELS,
    multi_label=False
)

output = {
    "stage": result["labels"][0],
    "confidence": float(result["scores"][0])
}

print(json.dumps(output))
