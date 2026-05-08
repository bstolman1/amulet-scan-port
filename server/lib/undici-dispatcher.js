import { Agent as UndiciAgent } from 'undici';

/**
 * Extract hostname from a full URL for Host header + TLS SNI.
 */
export function extractHostname(url) {
  try {
    return new URL(url).hostname;
  } catch {
    return undefined;
  }
}

const dispatcherCache = new Map();

/**
 * Get or create an Undici dispatcher for a hostname.
 * Dispatchers are cached so we reuse TCP/TLS connection pools
 * instead of creating a new agent (and new connections) per request.
 */
export function createDispatcher(hostname) {
  let dispatcher = dispatcherCache.get(hostname);
  if (!dispatcher) {
    dispatcher = new UndiciAgent({
      connect: {
        servername: hostname,
      },
      keepAliveTimeout: 10_000,
      keepAliveMaxTimeout: 10_000,
    });
    dispatcherCache.set(hostname, dispatcher);
  }
  return dispatcher;
}
