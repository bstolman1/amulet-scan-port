// This file MUST be imported first before any rate limiters
// ES module imports are hoisted, so we need a separate file
import express from 'express';

const app = express();

// 🔴 MUST come before any rate limiter is imported
// Trust only the local reverse proxy (nginx) to supply X-Forwarded-For.
// Using `true` is too permissive and express-rate-limit will abort.
app.set('trust proxy', 'loopback');

export default app;
