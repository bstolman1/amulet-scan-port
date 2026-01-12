# Classification System: Why This Exists

This document explains the architecture decisions behind the governance classification system. It exists to prevent future "simplifications" that might destroy the carefully balanced hybrid approach.

## Overview

The governance classification system identifies the type of each governance topic (CIP, Validator, Featured App, Protocol Upgrade, Outcome, etc.) using a layered approach that combines deterministic rules with machine learning.

## Architecture Layers

### 1. Regex/Keyword Layer (Deterministic)

**Why it exists:** Fast, predictable, auditable. When a topic contains "CIP-0054" or "Validator Approved: Figment", the regex layer can classify it instantly with 100% confidence.

**Properties:**
- Zero latency
- Perfect reproducibility
- Easy to audit/debug
- Covers ~70% of cases accurately

**Don't remove this because:** It provides the baseline. Without deterministic rules, every single classification would require LLM inference, which is:
- Slow (500ms+ per item)
- Expensive ($)
- Non-deterministic (same input can produce different outputs)
- Hard to debug

### 2. Learned Patterns Layer (Adaptive)

**Why it exists:** Human corrections teach the system new patterns without code changes.

**How it works:**
1. Human corrects a classification (e.g., "MyValidator" → validator, not featured-app)
2. System extracts patterns from corrections
3. Patterns are stored with confidence scores
4. Future items matching patterns get classified accordingly

**Properties:**
- Forward-only by default (doesn't reclassify existing items)
- Confidence decay (patterns fade without reinforcement)
- Version-controlled with rollback capability

**Don't remove this because:** It enables continuous improvement without code deploys. The alternative is constant code changes for each new entity name.

### 3. LLM Audit Layer (Semantic Verification)

**Why it exists:** To catch cases where rules get it wrong.

**Design principle:** "LLMs may read raw human text exactly once per artifact; results are cached and treated as authoritative governance metadata."

**How it works:**
1. Regex classifies everything first
2. LLM reads the full post content ONCE
3. LLM either confirms the regex result OR flags disagreement
4. Disagreements go to human review
5. Results are cached permanently (never re-run unless content changes)

**Properties:**
- Single-read design (cost-controlled)
- Permanent caching (reproducible)
- Hybrid consensus (rules + LLM must agree for high confidence)

**Don't remove this because:** Rules can't understand semantic nuance. "Splice Migration Discussion" could be a CIP or a Protocol Upgrade depending on context. The LLM reads the full content to disambiguate.

### 4. Human Review Layer (Ground Truth)

**Why it exists:** Humans are the ultimate authority on classification.

**Properties:**
- Corrections are logged to audit trail
- Corrections feed into learned patterns
- Corrections populate the golden evaluation set

**Don't remove this because:** It's the feedback loop that makes everything else work. Without human corrections, the system can't improve.

## Key Design Decisions

### Why Learning is Forward-Only

**Decision:** New patterns only affect future classifications, not existing ones.

**Why:**
1. **Stability:** Users don't see items randomly changing type
2. **Auditability:** Historical reports remain consistent
3. **Safety:** Bad patterns can't retroactively corrupt data

**Exception:** Manual "re-classify with patterns" action for explicit retroactive changes.

### Why Confidence Decay Exists

**Decision:** Pattern confidence decays over time without reinforcement.

**Why:**
1. **Language evolves:** "SV" might have meant Super Validator in 2024 but something else in 2026
2. **Prevents cruft:** Old patterns that don't match anymore fade away
3. **Healthy ecosystem:** Active patterns stay strong, stale ones decay

**Reinforcement:** When a pattern matches and the classification is NOT corrected, the pattern is "reinforced" (confidence boosted).

### Why No-Regression Policy Exists

**Decision:** Changes that cause regressions on the golden set are BLOCKED.

**Why:**
1. **Prevent oscillation:** Pattern A fixes 5 items but breaks 3 others
2. **Objective progress:** Every version must be strictly better or equal
3. **Credibility:** Stakeholders trust accuracy numbers

**Policy:**
- ❌ Any regression on golden set → BLOCKED
- ⚠️ Regressions outside golden set → require justification
- ✅ Improvements without regressions → auto-eligible

### Why Golden Evaluation Set is Fixed

**Decision:** The golden set never changes (except to fix labeling errors).

**Why:**
1. **Objective benchmark:** Same test, different classifiers, comparable results
2. **Regression detection:** If golden set changed, we couldn't detect regressions
3. **Credibility:** "Accuracy improved from 94.2% to 96.8%" is meaningful only if the test is fixed

**Contents:**
- ~50-150 hand-labeled items
- High-confidence, human-verified
- Covers edge cases and boundary cases
- Diverse across types

### Why Per-Decision Explainability Exists

**Decision:** Every classification has a stored trace showing WHY it was classified that way.

**Why:**
1. **Debugging:** "Why did this get classified as X?" is instantly answerable
2. **Audit:** Regulators/auditors can verify classification logic
3. **Trust:** Humans can review the reasoning, not just the result

**Contents:**
- Signals extracted (keywords, regex hits)
- Patterns applied (with confidence)
- LLM reasoning (if consulted)
- Step-by-step decision logic

## Anti-Patterns to Avoid

### DON'T: Remove the regex layer to "simplify"
The LLM-only approach would be slow, expensive, and non-deterministic.

### DON'T: Make learned patterns apply retroactively by default
This causes data instability and makes reports unreliable.

### DON'T: Remove confidence decay
Old patterns accumulate forever, causing false positives.

### DON'T: Skip human review for disagreements
The whole point of the hybrid system is human-in-the-loop.

### DON'T: Let the golden set evolve with the data
It must be fixed to provide objective benchmarking.

### DON'T: Delete decision traces "to save space"
They're your audit trail. Compress them, archive them, but don't delete them.

## Metrics to Track

1. **Golden set accuracy per version** (primary KPI)
2. **Regression count per version**
3. **Pattern count by type and confidence**
4. **LLM consultation rate** (should be <20%)
5. **Human correction rate** (should decrease over time)
6. **Decision trace coverage** (should be 100%)

## Version History

- v1.0.0: Initial regex-only classification
- v2.0.0: Added LLM audit layer
- v3.0.0: Added learned patterns with decay
- v4.0.0: Added golden evaluation set and no-regression policy
- v5.0.0: Added per-decision explainability

---

*This document should be updated when architectural decisions change. Last updated: 2026-01-12*
