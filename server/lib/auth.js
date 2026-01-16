/**
 * Authentication middleware for protecting API endpoints
 * 
 * Supports two authentication methods:
 * 1. API Key authentication (X-API-Key header)
 * 2. Admin API Key for administrative operations
 */

/**
 * Middleware to require API key authentication
 * Checks for X-API-Key header against ADMIN_API_KEY env variable
 */
export function requireAuth(req, res, next) {
  const apiKey = req.headers['x-api-key'];
  const adminKey = process.env.ADMIN_API_KEY;

  // If no admin key is configured, deny all requests
  if (!adminKey) {
    console.error('ADMIN_API_KEY environment variable not set - cannot authenticate');
    return res.status(500).json({
      error: 'Authentication not configured',
      message: 'Server authentication is not properly configured. Please contact the administrator.'
    });
  }

  // Check if API key is provided
  if (!apiKey) {
    return res.status(401).json({
      error: 'Authentication required',
      message: 'Please provide an API key via X-API-Key header'
    });
  }

  // Validate API key using constant-time comparison to prevent timing attacks
  if (!constantTimeCompare(apiKey, adminKey)) {
    return res.status(403).json({
      error: 'Invalid API key',
      message: 'The provided API key is invalid'
    });
  }

  // Authentication successful
  next();
}

/**
 * Optional authentication middleware - allows requests through but sets req.authenticated
 * Useful for endpoints that work differently for authenticated vs unauthenticated users
 */
export function optionalAuth(req, res, next) {
  const apiKey = req.headers['x-api-key'];
  const adminKey = process.env.ADMIN_API_KEY;

  if (adminKey && apiKey && constantTimeCompare(apiKey, adminKey)) {
    req.authenticated = true;
  } else {
    req.authenticated = false;
  }

  next();
}

/**
 * Constant-time string comparison to prevent timing attacks
 * @param {string} a - First string to compare
 * @param {string} b - Second string to compare
 * @returns {boolean} - True if strings match
 */
function constantTimeCompare(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') {
    return false;
  }

  // If lengths differ, still compare to prevent timing attacks
  const minLength = Math.min(a.length, b.length);
  const maxLength = Math.max(a.length, b.length);
  
  let result = a.length === b.length ? 0 : 1;

  for (let i = 0; i < maxLength; i++) {
    const aChar = i < a.length ? a.charCodeAt(i) : 0;
    const bChar = i < b.length ? b.charCodeAt(i) : 0;
    result |= aChar ^ bChar;
  }

  return result === 0;
}

/**
 * Generate a secure API key
 * This is a utility function to help administrators generate secure keys
 * Run with: node -e "import('./lib/auth.js').then(m => console.log(m.generateApiKey()))"
 */
export function generateApiKey() {
  const crypto = await import('crypto');
  return crypto.randomBytes(32).toString('hex');
}
