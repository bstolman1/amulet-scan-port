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

/**
 * Create an Undici dispatcher that fetch() actually uses (unlike https.Agent).
 * This ensures TLS handshake uses the correct SNI server name.
 */
export function createDispatcher(hostname) {
  return new UndiciAgent({
    connect: {
      servername: hostname,
    },
    keepAliveTimeout: 10_000,
    keepAliveMaxTimeout: 10_000,
  });
}
