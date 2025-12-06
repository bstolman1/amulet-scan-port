import express from 'express';
import cors from 'cors';
import eventsRouter from './api/events.js';
import partyRouter from './api/party.js';
import contractsRouter from './api/contracts.js';
import statsRouter from './api/stats.js';
import searchRouter from './api/search.js';
import backfillRouter from './api/backfill.js';
import acsRouter from './api/acs.js';

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// API routes
app.use('/api/events', eventsRouter);
app.use('/api/party', partyRouter);
app.use('/api/contracts', contractsRouter);
app.use('/api/stats', statsRouter);
app.use('/api/search', searchRouter);
app.use('/api/backfill', backfillRouter);
app.use('/api/acs', acsRouter);

app.listen(PORT, () => {
  console.log(`🦆 DuckDB API server running on http://localhost:${PORT}`);
  console.log(`📁 Reading data files from ../data/`);
});
