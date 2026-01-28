// This file MUST be imported first before any rate limiters
// ES module imports are hoisted, so we need a separate file
import express from 'express';

const app = express();

// 🔴 MUST come before any rate limiter is imported
// Trust exactly one proxy hop (e.g., nginx in front of this app).
// This keeps IP-based rate limiting secure while allowing X-Forwarded-For.
app.set('trust proxy', 1);

export default app;
