/**
 * textExtraction.js - stub
 */

export function extractUrls(text) {
  if (!text) return [];
  const urlRegex = /https?:\/\/[^\s"'<>)]+/g;
  return text.match(urlRegex) || [];
}

export function extractIdentifiers(text, learnedPatterns = []) {
  if (!text) return [];
  const found = new Set();

  // Common identifier patterns (e.g. draft-ietf-*, RFC-1234, etc.)
  const defaultPatterns = [
    /\bdraft-[a-z0-9-]+/gi,
    /\bRFC[-\s]?\d{3,5}\b/gi,
    /\bBCP[-\s]?\d+\b/gi,
  ];

  for (const pattern of defaultPatterns) {
    for (const match of text.matchAll(pattern)) {
      found.add(match[0].trim());
    }
  }

  for (const pattern of learnedPatterns) {
    try {
      const re = new RegExp(pattern, 'gi');
      for (const match of text.matchAll(re)) {
        found.add(match[0].trim());
      }
    } catch {}
  }

  return [...found];
}
