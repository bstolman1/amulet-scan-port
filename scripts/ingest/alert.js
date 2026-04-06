/**
 * Alerting Module for Canton Ingestion Pipeline
 *
 * Sends notifications via Slack webhook and/or email (SMTP) when critical
 * pipeline events occur. Designed to be non-blocking — alert failures are
 * logged but never crash the ingestion process.
 *
 * Configuration (env vars):
 *   ALERT_SLACK_WEBHOOK_URL  — Slack incoming webhook URL
 *   ALERT_EMAIL_ENABLED      — "true" to enable email alerts
 *   ALERT_SMTP_HOST          — SMTP server host
 *   ALERT_SMTP_PORT          — SMTP server port (default 587)
 *   ALERT_SMTP_USER          — SMTP username
 *   ALERT_SMTP_PASS          — SMTP password
 *   ALERT_SMTP_FROM          — Sender address (default: ALERT_SMTP_USER)
 *   ALERT_EMAIL_TO           — Comma-separated recipient addresses
 *   ALERT_RATE_LIMIT_MS      — Min interval between alerts of same type (default 300000 = 5min)
 *   ALERT_HOSTNAME           — Identifier for this host/instance (default: os.hostname())
 */

import axios from 'axios';
import os from 'os';

// ─── Configuration ────────────────────────────────────────────────────────

const SLACK_WEBHOOK_URL = process.env.ALERT_SLACK_WEBHOOK_URL || '';
const EMAIL_ENABLED     = process.env.ALERT_EMAIL_ENABLED === 'true';
const SMTP_HOST         = process.env.ALERT_SMTP_HOST || '';
const SMTP_PORT         = parseInt(process.env.ALERT_SMTP_PORT) || 587;
const SMTP_USER         = process.env.ALERT_SMTP_USER || '';
const SMTP_PASS         = process.env.ALERT_SMTP_PASS || '';
const SMTP_FROM         = process.env.ALERT_SMTP_FROM || SMTP_USER;
const EMAIL_TO          = (process.env.ALERT_EMAIL_TO || '').split(',').map(s => s.trim()).filter(Boolean);
const RATE_LIMIT_MS     = parseInt(process.env.ALERT_RATE_LIMIT_MS) || 300_000; // 5 min
const HOSTNAME          = process.env.ALERT_HOSTNAME || os.hostname();

// ─── Severity levels ──────────────────────────────────────────────────────

export const Severity = {
  INFO:     'info',
  WARNING:  'warning',
  CRITICAL: 'critical',
  FATAL:    'fatal',
};

const SEVERITY_EMOJI = {
  info:     'large_blue_circle',
  warning:  'warning',
  critical: 'red_circle',
  fatal:    'rotating_light',
};

const SEVERITY_COLOR = {
  info:     '#2196F3',
  warning:  '#FF9800',
  critical: '#F44336',
  fatal:    '#B71C1C',
};

// ─── Rate limiting ────────────────────────────────────────────────────────

const _lastAlertTimes = new Map();

function isRateLimited(alertType) {
  const now = Date.now();
  const last = _lastAlertTimes.get(alertType) || 0;
  if (now - last < RATE_LIMIT_MS) return true;
  _lastAlertTimes.set(alertType, now);
  return false;
}

// ─── Lazy nodemailer import ───────────────────────────────────────────────
// nodemailer is only imported when email is actually enabled, so the module
// works without it installed if only Slack is used.

let _nodemailer = null;
let _transporter = null;

async function getEmailTransporter() {
  if (_transporter) return _transporter;
  if (!_nodemailer) {
    try {
      _nodemailer = await import('nodemailer');
    } catch {
      console.error('[alert] nodemailer not installed — run: npm install nodemailer');
      return null;
    }
  }
  _transporter = _nodemailer.default.createTransport({
    host: SMTP_HOST,
    port: SMTP_PORT,
    secure: SMTP_PORT === 465,
    auth: (SMTP_USER && SMTP_PASS) ? { user: SMTP_USER, pass: SMTP_PASS } : undefined,
  });
  return _transporter;
}

// ─── Channel: Slack ───────────────────────────────────────────────────────

async function sendSlack(severity, title, details) {
  if (!SLACK_WEBHOOK_URL) return;

  const emoji = SEVERITY_EMOJI[severity] || 'grey_question';
  const color = SEVERITY_COLOR[severity] || '#9E9E9E';

  const fields = Object.entries(details)
    .filter(([, v]) => v !== undefined && v !== null)
    .map(([k, v]) => ({ title: k, value: String(v), short: String(v).length < 40 }));

  const payload = {
    attachments: [{
      color,
      fallback: `:${emoji}: [${severity.toUpperCase()}] ${title}`,
      pretext: `:${emoji}: *[${severity.toUpperCase()}] Canton Ingestion Alert*`,
      title,
      fields: [
        { title: 'Host', value: HOSTNAME, short: true },
        { title: 'Time', value: new Date().toISOString(), short: true },
        ...fields,
      ],
      footer: 'canton-ingestion-pipeline',
      ts: Math.floor(Date.now() / 1000),
    }],
  };

  try {
    const resp = await axios.post(SLACK_WEBHOOK_URL, payload, { timeout: 10_000 });
    if (resp.status !== 200) {
      console.error(`[alert] Slack returned HTTP ${resp.status}: ${resp.data}`);
    }
  } catch (err) {
    const detail = err.code || err.response?.status || '';
    console.error(`[alert] Slack send failed (${detail}): ${err.message}`);
  }
}

// ─── Channel: Email ───────────────────────────────────────────────────────

async function sendEmail(severity, title, details) {
  if (!EMAIL_ENABLED || EMAIL_TO.length === 0) return;

  const transporter = await getEmailTransporter();
  if (!transporter) return;

  const detailRows = Object.entries(details)
    .filter(([, v]) => v !== undefined && v !== null)
    .map(([k, v]) => `<tr><td style="padding:4px 12px;font-weight:bold">${k}</td><td style="padding:4px 12px">${String(v)}</td></tr>`)
    .join('\n');

  const color = SEVERITY_COLOR[severity] || '#9E9E9E';

  const html = `
    <div style="font-family:monospace;max-width:600px">
      <div style="background:${color};color:white;padding:12px 16px;border-radius:4px 4px 0 0">
        <strong>[${severity.toUpperCase()}]</strong> ${title}
      </div>
      <table style="width:100%;border-collapse:collapse;border:1px solid #ddd">
        <tr><td style="padding:4px 12px;font-weight:bold">Host</td><td style="padding:4px 12px">${HOSTNAME}</td></tr>
        <tr><td style="padding:4px 12px;font-weight:bold">Time</td><td style="padding:4px 12px">${new Date().toISOString()}</td></tr>
        ${detailRows}
      </table>
    </div>
  `;

  const subject = `[${severity.toUpperCase()}] Canton Ingestion: ${title}`;

  try {
    await transporter.sendMail({
      from: SMTP_FROM,
      to: EMAIL_TO.join(', '),
      subject,
      html,
      text: `${subject}\n\nHost: ${HOSTNAME}\nTime: ${new Date().toISOString()}\n\n` +
            Object.entries(details)
              .filter(([, v]) => v !== undefined && v !== null)
              .map(([k, v]) => `${k}: ${v}`)
              .join('\n'),
    });
  } catch (err) {
    console.error(`[alert] Email send failed: ${err.message}`);
  }
}

// ─── Public API ───────────────────────────────────────────────────────────

/**
 * Send an alert to all configured channels.
 *
 * @param {string} severity - One of Severity.INFO/WARNING/CRITICAL/FATAL
 * @param {string} alertType - Dedup key for rate limiting (e.g. 'stall_detected')
 * @param {string} title - Short human-readable title
 * @param {object} details - Key-value pairs with context
 * @returns {Promise<void>}
 */
export async function alert(severity, alertType, title, details = {}) {
  if (isRateLimited(alertType)) return;

  // Fire both channels concurrently; never let alert failures propagate
  await Promise.allSettled([
    sendSlack(severity, title, details),
    sendEmail(severity, title, details),
  ]);
}

/**
 * Check whether any alert channel is configured.
 */
export function isAlertingConfigured() {
  return !!(SLACK_WEBHOOK_URL || (EMAIL_ENABLED && EMAIL_TO.length > 0));
}

/**
 * Log alerting configuration at startup (redacts secrets).
 */
export function logAlertConfig() {
  const channels = [];
  if (SLACK_WEBHOOK_URL) channels.push('slack');
  if (EMAIL_ENABLED && EMAIL_TO.length > 0) channels.push(`email(${EMAIL_TO.join(',')})`);
  if (channels.length === 0) {
    console.log('  ALERTING: disabled (set ALERT_SLACK_WEBHOOK_URL or ALERT_EMAIL_* to enable)');
  } else {
    console.log(`  ALERTING: ${channels.join(' + ')} | rate_limit=${RATE_LIMIT_MS / 1000}s`);
  }
}

export default { alert, Severity, isAlertingConfigured, logAlertConfig };
