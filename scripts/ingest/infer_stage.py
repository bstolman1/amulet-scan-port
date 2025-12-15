#!/usr/bin/env python3
"""
Deterministic Local Governance Classification Engine

Hybrid pattern-matching + zero-shot NLI classification of governance messages.
Uses explicit subject line patterns for high-confidence cases, falls back to
NLI for ambiguous cases.

Input: JSONL via stdin - each line is {"id": "...", "text": "..."}
Output: JSONL to stdout - each line is {"id": "...", "stage": "...", "confidence": 0.XX}

All logs go to stderr. Output is machine-parsable JSONL only.
"""

import sys
import json
import re
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

# Pattern-aware descriptions that help NLI understand subject line conventions
STAGE_DESCRIPTIONS = {
    "cip-discuss": "a CIP discussion thread for gathering feedback on a Canton Improvement Proposal, often with subjects like 'CIP Discuss' or 'CIP-XXXX Discussion'",
    "cip-vote": "a CIP voting thread or vote proposal, typically with subjects containing 'Vote Proposal', 'CIP Vote', or 'voting on CIP'",
    "cip-announce": "a CIP announcement or approval notice, typically with subjects containing 'Approved', 'CIP Announcement', or confirming a CIP decision",
    "tokenomics": "a tokenomics discussion about featured apps, validators, token weights, or reward distribution",
    "tokenomics-announce": "a tokenomics announcement confirming featured app status, weight changes, or reward allocations",
    "sv-announce": "a Super Validator announcement, approval notice, or weight assignment for an SV",
    "other": "a general governance topic that does not fit the other specific categories"
}

# High-confidence pattern matching for unambiguous subject lines
# Order matters - more specific patterns should come first
PATTERN_RULES = [
    # ========== TOKENOMICS ANNOUNCE (most specific first) ==========
    # Validator/Operator approvals
    (r'validator\s+operators?\s+approved', 'tokenomics-announce', 0.98),
    (r'validators?\s+approved', 'tokenomics-announce', 0.95),
    (r'operators?\s+approved', 'tokenomics-announce', 0.95),
    (r'featured\s+apps?\s+approved', 'tokenomics-announce', 0.95),
    
    # Tokenomics outcomes
    (r'tokenomics\s+outcomes?', 'tokenomics-announce', 0.98),
    (r'outcome.*tokenomics', 'tokenomics-announce', 0.90),
    
    # Weight/reward announcements
    (r'weight\s+(change|update|assigned|approved)', 'tokenomics-announce', 0.90),
    (r'reward.*approved', 'tokenomics-announce', 0.85),
    
    # ========== TOKENOMICS DISCUSSION ==========
    # Featured app requests
    (r'new\s+featured\s+app\s+request', 'tokenomics', 0.98),
    (r'featured\s+app\s+request', 'tokenomics', 0.95),
    (r'featured\s+app.*discuss', 'tokenomics', 0.90),
    
    # Validator requests
    (r'new\s+validator\s+request', 'tokenomics', 0.95),
    (r'validator\s+operator\s+request', 'tokenomics', 0.95),
    (r'validator.*application', 'tokenomics', 0.85),
    
    # Weight discussions
    (r'weight.*request', 'tokenomics', 0.85),
    (r'setting\s+weights?\s+on', 'tokenomics', 0.90),
    
    # ========== CIP ANNOUNCE ==========
    (r'approved\s+by\s+sv', 'cip-announce', 0.98),
    (r'sv\s+rights?\s+owners?.*approved', 'cip-announce', 0.98),
    (r'cip[-\s]*\d+.*approved', 'cip-announce', 0.95),
    (r'cip\s*announcement', 'cip-announce', 0.95),
    (r'cip[-\s]*\d+.*announcement', 'cip-announce', 0.90),
    
    # ========== CIP VOTE ==========
    (r'vote\s+proposal\s+to\s+add', 'cip-vote', 0.95),  # "Vote Proposal to add X weight" is CIP vote
    (r'vote\s+proposal', 'cip-vote', 0.95),
    (r'cip[-\s]*\d+\s*:?\s*vote', 'cip-vote', 0.95),
    (r'cip\s+vote', 'cip-vote', 0.95),
    (r'voting\s+on\s+cip', 'cip-vote', 0.90),
    (r'cip[-\s]*\d+.*\bvote\b', 'cip-vote', 0.85),
    
    # ========== CIP DISCUSS ==========
    # Must come after vote/announce to not override them
    (r'cip[-\s]*discuss', 'cip-discuss', 0.98),
    (r'cip\s+discussion', 'cip-discuss', 0.95),
    (r'draft\s+cip', 'cip-discuss', 0.95),
    (r'cip[-\s]*tbd', 'cip-discuss', 0.90),  # CIP-TBD is discussion stage
    (r'cip[-\s]*00xx', 'cip-discuss', 0.90),  # CIP-00XX is discussion stage
    (r'cip[-\s]*xxxx', 'cip-discuss', 0.90),  # CIP-XXXX is discussion stage
    (r'cip\s*-\s*\d+\s*:', 'cip-discuss', 0.70),  # "CIP-0037:" without other keywords defaults to discuss
    
    # ========== SV ANNOUNCE ==========
    (r'sv\s+announcement', 'sv-announce', 0.95),
    (r'super\s*validator.*announc', 'sv-announce', 0.90),
    (r'add.*weight.*to.*gsf\s*sv', 'sv-announce', 0.90),
    (r'sv\s+weight\s+assignment', 'sv-announce', 0.90),
]

def quick_classify(text):
    """
    Pattern-based pre-classification for high-confidence cases.
    Returns (stage, confidence) or None if no pattern matches.
    """
    text_lower = text.lower()
    
    for pattern, stage, confidence in PATTERN_RULES:
        if re.search(pattern, text_lower):
            return (stage, confidence)
    
    return None

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
    pattern_hits = 0
    nli_hits = 0
    
    def process_batch(items):
        """Process a mini-batch and free memory"""
        nonlocal processed, pattern_hits, nli_hits
        for item in items:
            item_id = item.get("id", "unknown")
            text = item.get("text", "").strip()
            
            if not text:
                print(json.dumps({"id": item_id, "stage": "other", "confidence": 0.0}), flush=True)
                processed += 1
                continue
            
            # Try pattern-based classification first (high confidence)
            pattern_result = quick_classify(text)
            if pattern_result:
                stage, confidence = pattern_result
                print(json.dumps({
                    "id": item_id,
                    "stage": stage,
                    "confidence": round(confidence, 4)
                }), flush=True)
                processed += 1
                pattern_hits += 1
                continue
            
            try:
                # Fall back to NLI for ambiguous cases
                result = classifier(
                    text,
                    candidate_labels,
                    hypothesis_template="This governance forum post is {}.",
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
                nli_hits += 1
                
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
    
    log(f"Inference complete. Processed {processed} items (pattern: {pattern_hits}, NLI: {nli_hits}).")

if __name__ == "__main__":
    main()
