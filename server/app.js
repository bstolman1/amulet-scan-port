// This file MUST be imported first before any rate limiters
// ES module imports are hoisted, so we need a separate file
import express from 'express';

const app = express();

// 🔴 MUST come before any rate limiter is imported
app.set('trust proxy', true);

export default app;
