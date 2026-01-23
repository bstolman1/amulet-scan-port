
## Limit Kaiko Timeframes to 30 Days

Remove the 90D and MAX timeframe options from the CC Performance component so users only see the available data range without any indication of limitations.

### Changes

**File: `src/components/CCTimeframeComparison.tsx`**

| Line | Change |
|------|--------|
| 10 | Update type from `"1D" \| "7D" \| "30D" \| "90D" \| "MAX"` to `"1D" \| "7D" \| "30D"` |
| 12-18 | Remove 90D and MAX from the `TIMEFRAMES` array |
| 28-30 | Simplify the `startTimeFor` function to only handle 7D and 30D cases |

### Before/After

```text
┌─────────────────────────────────────────────┐
│  BEFORE                                     │
│  [1D] [7D] [30D] [90D] [Max]               │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│  AFTER                                      │
│  [1D] [7D] [30D]                            │
└─────────────────────────────────────────────┘
```

### Technical Details

The `startTimeFor` function simplification:

```typescript
// Before (line 28-30)
const days = timeframe === "7D" ? 7 : timeframe === "30D" ? 30 : timeframe === "90D" ? 90 : 365 * 3;
const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
return { startTime: start.toISOString(), interval: "1d", pageSize: timeframe === "MAX" ? 1100 : 200 };

// After
const days = timeframe === "7D" ? 7 : 30;
const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
return { startTime: start.toISOString(), interval: "1d", pageSize: 200 };
```

This is a clean removal with no explanatory text or badges - users will simply see the available timeframes as if that's all there ever was.
