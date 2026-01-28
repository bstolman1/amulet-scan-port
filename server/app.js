// This file MUST be imported first before any rate limiters
// ES module imports are hoisted, so we need a separate file
import express from 'express';

const app = express();

// 🔴 MUST come before any rate limiter is imported
// Trust only proxies from localhost (nginx on 127.0.0.1).
// Using 'loopback' satisfies express-rate-limit: it's specific (not permissive)
// and it's set (so X-Forwarded-For is trusted).
app.set('trust proxy', 'loopback');

export default app;
