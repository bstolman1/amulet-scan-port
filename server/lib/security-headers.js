/**
 * Security headers middleware (helmet-like functionality)
 * Implements essential security headers without external dependencies
 */

/**
 * Apply security headers to all responses
 * @param {Object} options - Configuration options
 * @returns {Function} Express middleware function
 */
export function securityHeaders(options = {}) {
  return function securityHeadersMiddleware(req, res, next) {
    // Prevent clickjacking attacks
    res.setHeader('X-Frame-Options', 'DENY');

    // Prevent MIME type sniffing
    res.setHeader('X-Content-Type-Options', 'nosniff');

    // Enable XSS protection (for older browsers)
    res.setHeader('X-XSS-Protection', '1; mode=block');

    // Referrer policy - control referrer information
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');

    // Permissions policy - restrict browser features
    res.setHeader('Permissions-Policy', 'geolocation=(), microphone=(), camera=()');

    // Content Security Policy (CSP) - prevent XSS and injection attacks
    const csp = options.contentSecurityPolicy || {
      directives: {
        'default-src': ["'self'"],
        'script-src': ["'self'", "'unsafe-inline'"],
        'style-src': ["'self'", "'unsafe-inline'"],
        'img-src': ["'self'", 'data:', 'https:'],
        'connect-src': ["'self'"],
        'font-src': ["'self'", 'data:'],
        'object-src': ["'none'"],
        'base-uri': ["'self'"],
        'form-action': ["'self'"],
        'frame-ancestors': ["'none'"],
        'upgrade-insecure-requests': []
      }
    };

    if (csp !== false) {
      const cspString = Object.entries(csp.directives)
        .map(([directive, sources]) => {
          if (sources.length === 0) return directive;
          return directive + ' ' + sources.join(' ');
        })
        .join('; ');
      res.setHeader('Content-Security-Policy', cspString);
    }

    // HSTS - Force HTTPS (only set if already on HTTPS)
    if (req.secure || req.headers['x-forwarded-proto'] === 'https') {
      res.setHeader(
        'Strict-Transport-Security',
        'max-age=31536000; includeSubDomains; preload'
      );
    }

    // Remove powered-by header to reduce information disclosure
    res.removeHeader('X-Powered-By');

    next();
  };
}

/**
 * Middleware to redirect HTTP to HTTPS in production
 */
export function requireHTTPS(req, res, next) {
  if (!req.secure && req.headers['x-forwarded-proto'] !== 'https') {
    if (process.env.NODE_ENV === 'production') {
      return res.redirect(301, 'https://' + req.headers.host + req.url);
    }
  }
  next();
}
