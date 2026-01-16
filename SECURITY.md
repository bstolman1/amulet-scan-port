# Security Fixes and Recommendations

This document outlines the security improvements made to the Amulet Scan application and remaining tasks for production deployment.

## ✅ Critical Issues Fixed

### 1. Sensitive Files Protection
- **Issue**: `.env` files with API keys were committed to git
- **Fix Applied**:
  - Removed `.env` files from git tracking (keys remain in working directory)
  - Added `.env` patterns to `.gitignore`
  - Created `.env.example` template files in `/`, `/server/`, and `/scripts/ingest/`
- **⚠️ ACTION REQUIRED**: 
  - **IMMEDIATELY revoke and rotate** all exposed API keys:
    - OpenAI API key
    - Kaiko API key
    - Groups.io API key
    - Supabase keys
  - Consider using a secrets management service (AWS Secrets Manager, HashiCorp Vault, etc.)
  - Run `git filter-branch` or BFG Repo-Cleaner to remove secrets from git history

### 2. Authentication and Authorization
- **Issue**: No authentication on any endpoints - anyone could trigger admin operations
- **Fix Applied**:
  - Created authentication middleware (`server/lib/auth.js`)
  - Protected all admin endpoints with `requireAuth` middleware:
    - `POST /api/refresh-views`
    - `POST /api/refresh-aggregations`
    - `POST /api/stats/init-engine-schema`
    - `POST /api/stats/aggregation-state/reset`
    - `DELETE /api/stats/live-cursor`
    - `POST /api/acs/cache/invalidate`
    - `POST /api/acs/trigger-snapshot`
    - `POST /api/acs/purge`
    - `DELETE /api/backfill/purge`
    - `POST /api/backfill/gaps/detect`
    - `POST /api/backfill/validate-integrity`
    - `POST /api/backfill/gaps/recover`
    - All governance-lifecycle POST/DELETE endpoints (23 total)
- **⚠️ ACTION REQUIRED**:
  - Generate a secure admin API key:
    ```bash
    openssl rand -hex 32
    ```
  - Add to your `.env` files:
    ```
    ADMIN_API_KEY=your-generated-key-here
    ```
  - To use admin endpoints, include header:
    ```
    X-API-Key: your-admin-api-key
    ```

### 3. CORS Misconfiguration
- **Issue**: Unrestricted CORS allowed any website to access the API
- **Fix Applied**:
  - CORS now validates origin against whitelist
  - Default development origins: `http://localhost:5173`, `http://localhost:3000`
- **⚠️ ACTION REQUIRED**:
  - Set production origins in `.env`:
    ```
    ALLOWED_ORIGINS=https://yourdomain.com,https://www.yourdomain.com
    ```

### 4. Information Disclosure
- **Issue**: `/health/config` endpoint exposed internal paths, Node.js version, and environment details
- **Fix Applied**: 
  - Removed `/health/config` endpoint entirely
  - Configuration debugging should use server logs instead

## ✅ High Priority Issues Fixed

### 5. Rate Limiting
- **Issue**: No rate limiting - vulnerable to DoS attacks
- **Fix Applied**:
  - Created custom rate limiting middleware (`server/lib/rate-limit.js`)
  - API endpoints limited to 100 requests per 15 minutes per IP
  - Admin endpoints limited to 20 requests per 15 minutes per IP
  - Rate limit headers included in responses

### 6. Security Headers
- **Issue**: Missing all security headers
- **Fix Applied**:
  - Created security headers middleware (`server/lib/security-headers.js`)
  - Headers now included:
    - `X-Frame-Options: DENY` (prevents clickjacking)
    - `X-Content-Type-Options: nosniff` (prevents MIME sniffing)
    - `X-XSS-Protection: 1; mode=block` (XSS protection)
    - `Referrer-Policy: strict-origin-when-cross-origin`
    - `Permissions-Policy: geolocation=(), microphone=(), camera=()`
    - `Content-Security-Policy` (prevents XSS and injection)
    - `Strict-Transport-Security` (HSTS - when on HTTPS)
  - Removed `X-Powered-By` header

### 7. HTTPS Enforcement
- **Issue**: No HTTPS enforcement
- **Fix Applied**:
  - Added HTTPS redirect middleware (active in production)
  - HSTS header set when on HTTPS
- **⚠️ ACTION REQUIRED**:
  - Deploy behind reverse proxy (nginx, Apache) with SSL/TLS certificate
  - Or use managed platform (Heroku, AWS ELB, etc.) with HTTPS

## ⚠️ Remaining Issues (Manual Intervention Required)

### 8. Vulnerable Dependencies
- **Status**: NOT FIXED - npm install failed due to infrastructure issues
- **Vulnerabilities**:
  - `glob` (high severity) - Command injection via CLI
  - `esbuild` (moderate) - Dev server response leakage  
  - `js-yaml` (moderate) - Prototype pollution
  - `vite` (moderate) - Multiple vulnerabilities
- **⚠️ ACTION REQUIRED**:
  - Run `npm audit fix` in a clean environment
  - Update packages manually:
    ```bash
    npm update glob esbuild js-yaml vite
    ```

### 9. Prompt Injection Risk
- **Location**: `server/inference/llm-classifier.js:215-224`
- **Issue**: User content injected directly into LLM prompts
- **Recommendation**:
  - Sanitize user input before LLM processing
  - Use structured JSON input format
  - Implement content filtering
  - Add output validation

### 10. Excessive Logging
- **Issue**: 279 `console.log` statements across 28 files
- **Recommendation**:
  - Replace with proper logging library (winston, pino)
  - Implement log levels (debug, info, warn, error)
  - Disable debug logs in production
  - Sanitize logs to prevent secret leakage

## 📋 Production Deployment Checklist

### Before Deployment
- [ ] Revoke and rotate all exposed API keys
- [ ] Remove `.env` files from git history (BFG Repo-Cleaner)
- [ ] Generate and set `ADMIN_API_KEY` in environment
- [ ] Set `ALLOWED_ORIGINS` for production domains
- [ ] Update vulnerable dependencies
- [ ] Set `NODE_ENV=production`
- [ ] Configure HTTPS/SSL certificate
- [ ] Set up monitoring and alerting
- [ ] Review and test all authentication endpoints

### Infrastructure
- [ ] Deploy behind reverse proxy with SSL termination
- [ ] Set up firewall rules
- [ ] Configure DDoS protection
- [ ] Set up log aggregation
- [ ] Implement backup strategy
- [ ] Configure health checks for load balancer

### Monitoring
- [ ] Set up error tracking (Sentry, Rollbar, etc.)
- [ ] Monitor rate limit hits
- [ ] Track authentication failures
- [ ] Set alerts for unusual traffic patterns
- [ ] Monitor disk space (data files can grow large)

## 🔐 Security Best Practices

### API Key Management
- Never commit API keys to version control
- Use environment variables or secrets manager
- Rotate keys regularly (quarterly recommended)
- Use different keys for dev/staging/production
- Implement key rotation without downtime

### Authentication Headers
To access protected admin endpoints:
```bash
curl -H "X-API-Key: your-admin-key" https://yourdomain.com/api/refresh-views
```

### Rate Limit Headers
Responses include:
- `X-RateLimit-Limit`: Maximum requests allowed
- `X-RateLimit-Remaining`: Requests remaining in window
- `X-RateLimit-Reset`: When the limit resets (ISO 8601)

### Security Testing
Before going live:
1. Run security scan (OWASP ZAP, Burp Suite)
2. Test authentication bypass attempts
3. Verify rate limiting effectiveness
4. Check CORS configuration
5. Verify HTTPS redirect works
6. Test with invalid/malicious input

## 📞 Incident Response

If you suspect a security breach:
1. Immediately rotate all API keys
2. Check server logs for suspicious activity
3. Review rate limit logs for unusual patterns
4. Check authentication failure logs
5. Consider temporary API shutdown if needed

## 📚 Additional Resources

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [Express Security Best Practices](https://expressjs.com/en/advanced/best-practice-security.html)
- [Node.js Security Checklist](https://blog.risingstack.com/node-js-security-checklist/)

---

**Last Updated**: 2026-01-16  
**Security Review By**: Claude Code Agent
