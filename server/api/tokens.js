import { Router } from 'express';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { extractHostname, createDispatcher } from '../lib/undici-dispatcher.js';

const router = Router();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const MAX_RESPONSE_BYTES = 5 * 1024 * 1024; // 5 MB
const REFRESH_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24 hours
const REQUEST_TIMEOUT_MS = 15_000;
const MAX_INSTRUMENTS_PER_REGISTRY = 500;

const GITHUB_ASSETS_URL =
  'https://raw.githubusercontent.com/canton-network/wallet/main/api-specs/assets.json';

const ENVIRONMENT = process.env.CANTON_ENVIRONMENT || 'MainNet';

// --- Seed data (mutable — updated from GitHub on each refresh) ---

const localAssets = JSON.parse(
  readFileSync(join(__dirname, '../data/assets.json'), 'utf-8')
);

let seedList = localAssets[ENVIRONMENT] || localAssets['MainNet'] || [];
let seedIndex = new Map();
let allowedHostnames = new Set(['raw.githubusercontent.com']);

function rebuildSeedIndex(list) {
  seedList = list;
  seedIndex = new Map();
  for (const asset of seedList) {
    seedIndex.set(`${asset.instrumentId.admin}::${asset.instrumentId.id}`, asset);
  }
  allowedHostnames = new Set(['raw.githubusercontent.com']);
  for (const asset of seedList) {
    for (const url of asset.registryURLs || []) {
      const h = extractHostname(url);
      if (h) allowedHostnames.add(h);
    }
  }
}

rebuildSeedIndex(seedList);
console.log(`[Tokens] Loaded ${seedList.length} seed assets for ${ENVIRONMENT}, ${allowedHostnames.size} allowed hostnames`);

// --- In-memory cache ---

let tokenCache = [];
let lastRefreshed = null;
let refreshInProgress = false;
let assetsSource = 'local';

function getUniqueRegistries() {
  const registries = new Map();
  for (const asset of seedList) {
    const admin = asset.instrumentId.admin;
    if (registries.has(admin)) continue;
    const urls = asset.registryURLs || [];
    if (urls.length > 0) {
      registries.set(admin, urls);
    }
  }
  return registries;
}

// --- HTTP helpers ---

async function readBodyWithLimit(response, maxBytes) {
  const reader = response.body?.getReader();
  if (!reader) return response.text();

  const chunks = [];
  let totalBytes = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      if (totalBytes > maxBytes) {
        reader.cancel();
        throw new Error(`Response exceeds ${maxBytes} byte limit`);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const combined = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(combined);
}

async function registryFetch(url) {
  const hostname = extractHostname(url);
  if (!hostname || !allowedHostnames.has(hostname)) {
    throw new Error(`Hostname not in allowlist: ${hostname}`);
  }
  const dispatcher = createDispatcher(hostname);
  const res = await fetch(url, {
    method: 'GET',
    headers: { Accept: 'application/json' },
    dispatcher,
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!res.ok) {
    const text = await readBodyWithLimit(res, 4096).catch(() => '');
    throw new Error(`HTTP ${res.status}: ${text.slice(0, 200)}`);
  }
  const text = await readBodyWithLimit(res, MAX_RESPONSE_BYTES);
  return JSON.parse(text);
}

function normalizeRegistryUrl(url) {
  let normalized = url.endsWith('/') ? url : url + '/';
  // SV scan servers serve registry at /registry/, not /api/scan/registry/
  normalized = normalized.replace(/\/api\/scan\/registry\//, '/registry/');
  return normalized;
}

// --- GitHub assets.json fetch ---

async function fetchAssetsFromGitHub() {
  try {
    console.log('[Tokens] Fetching assets.json from GitHub...');
    const res = await fetch(GITHUB_ASSETS_URL, {
      headers: { Accept: 'application/json' },
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    const text = await readBodyWithLimit(res, MAX_RESPONSE_BYTES);
    const data = JSON.parse(text);
    const list = data[ENVIRONMENT] || data['MainNet'] || [];
    if (!Array.isArray(list) || list.length === 0) {
      throw new Error('Empty or invalid asset list');
    }
    console.log(`[Tokens] GitHub assets.json: ${list.length} assets for ${ENVIRONMENT}`);
    rebuildSeedIndex(list);
    assetsSource = 'github';
  } catch (err) {
    console.warn(`[Tokens] GitHub fetch failed, using local fallback: ${err.message}`);
    assetsSource = 'local';
  }
}

// --- Discovery & refresh ---

async function fetchRegistryInstruments(registryUrls) {
  for (const rawUrl of registryUrls) {
    const base = normalizeRegistryUrl(rawUrl);
    try {
      const data = await registryFetch(`${base}metadata/v1/instruments`);
      const instruments = (data.instruments || []).slice(0, MAX_INSTRUMENTS_PER_REGISTRY);
      return { instruments, registryUrl: base, error: null };
    } catch (err) {
      console.warn(`[Tokens] Failed ${rawUrl}: ${err.message}`);
    }
  }
  return { instruments: [], registryUrl: null, error: 'All registry URLs failed' };
}

async function refreshTokenData() {
  if (refreshInProgress) {
    console.log('[Tokens] Refresh already in progress, skipping');
    return;
  }
  refreshInProgress = true;
  const startTime = Date.now();
  console.log('[Tokens] Starting token data refresh...');

  try {
    // Step 1: refresh seed from GitHub (updates seedList, seedIndex, allowedHostnames)
    await fetchAssetsFromGitHub();

    // Step 2: query each registry for live instrument data
    const registries = getUniqueRegistries();
    const allTokens = new Map();

    const registryResults = await Promise.allSettled(
      Array.from(registries.entries()).map(async ([admin, urls]) => {
        const { instruments, registryUrl, error } = await fetchRegistryInstruments(urls);
        if (error) {
          console.warn(`[Tokens] Registry ${admin.split('::')[0]}: ${error}`);
          return { admin, instruments: [], registryUrl: null, error };
        }
        console.log(`[Tokens] Registry ${admin.split('::')[0]}: ${instruments.length} instruments`);
        return { admin, instruments, registryUrl, error: null };
      })
    );

    for (const result of registryResults) {
      if (result.status !== 'fulfilled') continue;
      const { admin, instruments, registryUrl, error } = result.value;

      for (const inst of instruments) {
        const key = `${admin}::${inst.id}`;
        const seedEntry = seedIndex.get(key);

        allTokens.set(key, {
          instrumentId: { admin, id: inst.id },
          symbol: inst.symbol || seedEntry?.symbol || inst.id,
          name: inst.name || inst.id,
          totalSupply: inst.totalSupply || null,
          totalSupplyAsOf: inst.totalSupplyAsOf || null,
          decimals: inst.decimals ?? 10,
          supportedApis: inst.supportedApis || {},
          registryURLs: seedEntry?.registryURLs || (registryUrl ? [registryUrl] : []),
          linkToDAR: seedEntry?.linkToDAR || null,
          assetLogo: seedEntry?.assetLogo || null,
          source: seedEntry ? 'seed' : 'discovered',
          registryHealth: error ? 'error' : 'ok',
          issuer: admin.split('::')[0],
        });
      }

      // Include seed entries whose registry failed
      if (error) {
        for (const [seedKey, seedEntry] of seedIndex) {
          if (seedEntry.instrumentId.admin === admin && !allTokens.has(seedKey)) {
            allTokens.set(seedKey, {
              instrumentId: seedEntry.instrumentId,
              symbol: seedEntry.symbol,
              name: seedEntry.instrumentId.id,
              totalSupply: null,
              totalSupplyAsOf: null,
              decimals: 10,
              supportedApis: {},
              registryURLs: seedEntry.registryURLs || [],
              linkToDAR: seedEntry.linkToDAR || null,
              assetLogo: seedEntry.assetLogo || null,
              source: 'seed',
              registryHealth: 'error',
              issuer: seedEntry.instrumentId.admin.split('::')[0],
            });
          }
        }
      }
    }

    tokenCache = Array.from(allTokens.values()).sort((a, b) => {
      const supplyA = parseFloat(a.totalSupply) || 0;
      const supplyB = parseFloat(b.totalSupply) || 0;
      return supplyB - supplyA;
    });
    lastRefreshed = new Date().toISOString();

    const seedCount = tokenCache.filter(t => t.source === 'seed').length;
    const discoveredCount = tokenCache.filter(t => t.source === 'discovered').length;
    console.log(`[Tokens] Refresh complete in ${Date.now() - startTime}ms: ${tokenCache.length} tokens (${seedCount} seed, ${discoveredCount} discovered, assets from ${assetsSource})`);
  } catch (err) {
    console.error('[Tokens] Refresh failed:', err.message);
    if (tokenCache.length === 0) {
      tokenCache = seedList.map(asset => ({
        instrumentId: asset.instrumentId,
        symbol: asset.symbol,
        name: asset.instrumentId.id,
        totalSupply: null,
        totalSupplyAsOf: null,
        decimals: 10,
        supportedApis: {},
        registryURLs: asset.registryURLs || [],
        linkToDAR: asset.linkToDAR || null,
        assetLogo: asset.assetLogo || null,
        source: 'seed',
        registryHealth: 'pending',
        issuer: asset.instrumentId.admin.split('::')[0],
      }));
      lastRefreshed = new Date().toISOString();
    }
  } finally {
    refreshInProgress = false;
  }
}

// --- Startup + periodic refresh ---

refreshTokenData();
setInterval(refreshTokenData, REFRESH_INTERVAL_MS);

// --- Routes ---

router.get('/', (req, res) => {
  const seedCount = tokenCache.filter(t => t.source === 'seed').length;
  const discoveredCount = tokenCache.filter(t => t.source === 'discovered').length;
  res.json({
    tokens: tokenCache,
    lastRefreshed,
    environment: ENVIRONMENT,
    assetsSource,
    sources: { seed: seedCount, discovered: discoveredCount },
  });
});

router.get('/:id', (req, res) => {
  const id = req.params.id;
  const token = tokenCache.find(
    t => t.instrumentId.id === id || `${t.instrumentId.admin}::${t.instrumentId.id}` === id
  );
  if (!token) {
    return res.status(404).json({ error: `Token not found: ${id}` });
  }
  res.json(token);
});

router.post('/refresh', async (req, res) => {
  try {
    await refreshTokenData();
    res.json({ status: 'ok', tokenCount: tokenCache.length, lastRefreshed, assetsSource });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
