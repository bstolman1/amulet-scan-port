/**
 * Simple rate limiting middleware without external dependencies
 * Tracks requests per IP address and enforces rate limits
 */

// Store for tracking requests: { ip: { count: number, resetTime: number } }
const requestStore = new Map();

// Cleanup old entries every 5 minutes
setInterval(() => {
  const now = Date.now();
  for (const [ip, data] of requestStore.entries()) {
    if (now > data.resetTime) {
      requestStore.delete(ip);
    }
  }
}, 5 * 60 * 1000);

/**
 * Create a rate limiter middleware
 * @param {Object} options - Rate limit options
 * @param {number} options.windowMs - Time window in milliseconds (default: 15 minutes)
 * @param {number} options.max - Maximum number of requests per window (default: 100)
 * @param {string} options.message - Error message to return when limit exceeded
 * @returns {Function} Express middleware function
 */
export function rateLimit(options = {}) {
  const windowMs = options.windowMs || 15 * 60 * 1000; // 15 minutes
  const max = options.max || 100; // 100 requests
  const message = options.message || 'Too many requests, please try again later.';

  return function rateLimitMiddleware(req, res, next) {
    // Get client IP (consider proxy headers)
    const ip = req.ip || 
               req.headers['x-forwarded-for']?.split(',')[0]?.trim() || 
               req.headers['x-real-ip'] || 
               req.connection?.remoteAddress ||
               'unknown';

    const now = Date.now();
    let record = requestStore.get(ip);

    // Initialize or reset if window expired
    if (!record || now > record.resetTime) {
      record = {
        count: 0,
        resetTime: now + windowMs
      };
      requestStore.set(ip, record);
    }

    // Increment request count
    record.count++;

    // Set rate limit headers
    res.setHeader('X-RateLimit-Limit', max);
    res.setHeader('X-RateLimit-Remaining', Math.max(0, max - record.count));
    res.setHeader('X-RateLimit-Reset', new Date(record.resetTime).toISOString());

    // Check if limit exceeded
    if (record.count > max) {
      return res.status(429).json({
        error: 'Rate limit exceeded',
        message: message,
        retryAfter: Math.ceil((record.resetTime - now) / 1000)
      });
    }

    next();
  };
}

/**
 * Get current rate limit stats (for monitoring)
 */
export function getRateLimitStats() {
  return {
    totalIPs: requestStore.size,
    records: Array.from(requestStore.entries()).map(([ip, data]) => ({
      ip: ip.replace(/\d+\.\d+\.\d+\./, 'xxx.xxx.xxx.'), // Mask IP for privacy
      count: data.count,
      resetTime: new Date(data.resetTime).toISOString()
    }))
  };
}
