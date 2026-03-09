/**
 * SQL Sanitization Utilities
 * 
 * Provides input validation and sanitization for DuckDB queries.
 * CRITICAL: Use these functions to prevent SQL injection attacks.
 */
// @ts-nocheck


// Dangerous SQL patterns that should be rejected entirely
function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
const DANGEROUS_PATTERNS = stryMutAct_9fa48("413") ? [] : (stryCov_9fa48("413"), [// Statement injection patterns
stryMutAct_9fa48("415") ? /;\S*--/i : stryMutAct_9fa48("414") ? /;\s--/i : (stryCov_9fa48("414", "415"), /;\s*--/i), // Comment after statement terminator
stryMutAct_9fa48("417") ? /;\S*\/\*/i : stryMutAct_9fa48("416") ? /;\s\/\*/i : (stryCov_9fa48("416", "417"), /;\s*\/\*/i), // Block comment after statement terminator
stryMutAct_9fa48("421") ? /'\s*;\S*DROP/i : stryMutAct_9fa48("420") ? /'\s*;\sDROP/i : stryMutAct_9fa48("419") ? /'\S*;\s*DROP/i : stryMutAct_9fa48("418") ? /'\s;\s*DROP/i : (stryCov_9fa48("418", "419", "420", "421"), /'\s*;\s*DROP/i), // DROP statement injection
stryMutAct_9fa48("425") ? /'\s*;\S*DELETE/i : stryMutAct_9fa48("424") ? /'\s*;\sDELETE/i : stryMutAct_9fa48("423") ? /'\S*;\s*DELETE/i : stryMutAct_9fa48("422") ? /'\s;\s*DELETE/i : (stryCov_9fa48("422", "423", "424", "425"), /'\s*;\s*DELETE/i), // DELETE statement injection
stryMutAct_9fa48("429") ? /'\s*;\S*UPDATE/i : stryMutAct_9fa48("428") ? /'\s*;\sUPDATE/i : stryMutAct_9fa48("427") ? /'\S*;\s*UPDATE/i : stryMutAct_9fa48("426") ? /'\s;\s*UPDATE/i : (stryCov_9fa48("426", "427", "428", "429"), /'\s*;\s*UPDATE/i), // UPDATE statement injection
stryMutAct_9fa48("433") ? /'\s*;\S*INSERT/i : stryMutAct_9fa48("432") ? /'\s*;\sINSERT/i : stryMutAct_9fa48("431") ? /'\S*;\s*INSERT/i : stryMutAct_9fa48("430") ? /'\s;\s*INSERT/i : (stryCov_9fa48("430", "431", "432", "433"), /'\s*;\s*INSERT/i), // INSERT statement injection
stryMutAct_9fa48("437") ? /'\s*;\S*ALTER/i : stryMutAct_9fa48("436") ? /'\s*;\sALTER/i : stryMutAct_9fa48("435") ? /'\S*;\s*ALTER/i : stryMutAct_9fa48("434") ? /'\s;\s*ALTER/i : (stryCov_9fa48("434", "435", "436", "437"), /'\s*;\s*ALTER/i), // ALTER statement injection
stryMutAct_9fa48("441") ? /'\s*;\S*CREATE/i : stryMutAct_9fa48("440") ? /'\s*;\sCREATE/i : stryMutAct_9fa48("439") ? /'\S*;\s*CREATE/i : stryMutAct_9fa48("438") ? /'\s;\s*CREATE/i : (stryCov_9fa48("438", "439", "440", "441"), /'\s*;\s*CREATE/i), // CREATE statement injection
stryMutAct_9fa48("445") ? /'\s*;\S*TRUNCATE/i : stryMutAct_9fa48("444") ? /'\s*;\sTRUNCATE/i : stryMutAct_9fa48("443") ? /'\S*;\s*TRUNCATE/i : stryMutAct_9fa48("442") ? /'\s;\s*TRUNCATE/i : (stryCov_9fa48("442", "443", "444", "445"), /'\s*;\s*TRUNCATE/i), // TRUNCATE statement injection
stryMutAct_9fa48("449") ? /'\s*;\S*EXEC/i : stryMutAct_9fa48("448") ? /'\s*;\sEXEC/i : stryMutAct_9fa48("447") ? /'\S*;\s*EXEC/i : stryMutAct_9fa48("446") ? /'\s;\s*EXEC/i : (stryCov_9fa48("446", "447", "448", "449"), /'\s*;\s*EXEC/i), // EXEC statement injection
// UNION-based injection
stryMutAct_9fa48("451") ? /UNION\S+SELECT/i : stryMutAct_9fa48("450") ? /UNION\sSELECT/i : (stryCov_9fa48("450", "451"), /UNION\s+SELECT/i), // UNION injection
stryMutAct_9fa48("453") ? /UNION\S+ALL/i : stryMutAct_9fa48("452") ? /UNION\sALL/i : (stryCov_9fa48("452", "453"), /UNION\s+ALL/i), // UNION ALL injection
// Tautology-based injection (always-true conditions)
stryMutAct_9fa48("465") ? /'\s*OR\s+'[^']*'\s*=\s*'[']*'/i : stryMutAct_9fa48("464") ? /'\s*OR\s+'[^']*'\s*=\s*'[^']'/i : stryMutAct_9fa48("463") ? /'\s*OR\s+'[^']*'\s*=\S*'[^']*'/i : stryMutAct_9fa48("462") ? /'\s*OR\s+'[^']*'\s*=\s'[^']*'/i : stryMutAct_9fa48("461") ? /'\s*OR\s+'[^']*'\S*=\s*'[^']*'/i : stryMutAct_9fa48("460") ? /'\s*OR\s+'[^']*'\s=\s*'[^']*'/i : stryMutAct_9fa48("459") ? /'\s*OR\s+'[']*'\s*=\s*'[^']*'/i : stryMutAct_9fa48("458") ? /'\s*OR\s+'[^']'\s*=\s*'[^']*'/i : stryMutAct_9fa48("457") ? /'\s*OR\S+'[^']*'\s*=\s*'[^']*'/i : stryMutAct_9fa48("456") ? /'\s*OR\s'[^']*'\s*=\s*'[^']*'/i : stryMutAct_9fa48("455") ? /'\S*OR\s+'[^']*'\s*=\s*'[^']*'/i : stryMutAct_9fa48("454") ? /'\sOR\s+'[^']*'\s*=\s*'[^']*'/i : (stryCov_9fa48("454", "455", "456", "457", "458", "459", "460", "461", "462", "463", "464", "465"), /'\s*OR\s+'[^']*'\s*=\s*'[^']*'/i), // 'x' OR 'a'='a'
stryMutAct_9fa48("477") ? /'\s*OR\s+\d+\s*=\s*\D+/i : stryMutAct_9fa48("476") ? /'\s*OR\s+\d+\s*=\s*\d/i : stryMutAct_9fa48("475") ? /'\s*OR\s+\d+\s*=\S*\d+/i : stryMutAct_9fa48("474") ? /'\s*OR\s+\d+\s*=\s\d+/i : stryMutAct_9fa48("473") ? /'\s*OR\s+\d+\S*=\s*\d+/i : stryMutAct_9fa48("472") ? /'\s*OR\s+\d+\s=\s*\d+/i : stryMutAct_9fa48("471") ? /'\s*OR\s+\D+\s*=\s*\d+/i : stryMutAct_9fa48("470") ? /'\s*OR\s+\d\s*=\s*\d+/i : stryMutAct_9fa48("469") ? /'\s*OR\S+\d+\s*=\s*\d+/i : stryMutAct_9fa48("468") ? /'\s*OR\s\d+\s*=\s*\d+/i : stryMutAct_9fa48("467") ? /'\S*OR\s+\d+\s*=\s*\d+/i : stryMutAct_9fa48("466") ? /'\sOR\s+\d+\s*=\s*\d+/i : (stryCov_9fa48("466", "467", "468", "469", "470", "471", "472", "473", "474", "475", "476", "477"), /'\s*OR\s+\d+\s*=\s*\d+/i), // 'x' OR 1=1
stryMutAct_9fa48("483") ? /\bOR\s+1\s*=\S*1\b/i : stryMutAct_9fa48("482") ? /\bOR\s+1\s*=\s1\b/i : stryMutAct_9fa48("481") ? /\bOR\s+1\S*=\s*1\b/i : stryMutAct_9fa48("480") ? /\bOR\s+1\s=\s*1\b/i : stryMutAct_9fa48("479") ? /\bOR\S+1\s*=\s*1\b/i : stryMutAct_9fa48("478") ? /\bOR\s1\s*=\s*1\b/i : (stryCov_9fa48("478", "479", "480", "481", "482", "483"), /\bOR\s+1\s*=\s*1\b/i), // OR 1=1
stryMutAct_9fa48("491") ? /\bOR\s+'.*'\s*=\s*'.'/i : stryMutAct_9fa48("490") ? /\bOR\s+'.*'\s*=\S*'.*'/i : stryMutAct_9fa48("489") ? /\bOR\s+'.*'\s*=\s'.*'/i : stryMutAct_9fa48("488") ? /\bOR\s+'.*'\S*=\s*'.*'/i : stryMutAct_9fa48("487") ? /\bOR\s+'.*'\s=\s*'.*'/i : stryMutAct_9fa48("486") ? /\bOR\s+'.'\s*=\s*'.*'/i : stryMutAct_9fa48("485") ? /\bOR\S+'.*'\s*=\s*'.*'/i : stryMutAct_9fa48("484") ? /\bOR\s'.*'\s*=\s*'.*'/i : (stryCov_9fa48("484", "485", "486", "487", "488", "489", "490", "491"), /\bOR\s+'.*'\s*=\s*'.*'/i), // OR 'a'='a'
stryMutAct_9fa48("493") ? /\bOR\S+true\b/i : stryMutAct_9fa48("492") ? /\bOR\strue\b/i : (stryCov_9fa48("492", "493"), /\bOR\s+true\b/i), // OR true
stryMutAct_9fa48("499") ? /\bAND\s+1\s*=\S*0\b/i : stryMutAct_9fa48("498") ? /\bAND\s+1\s*=\s0\b/i : stryMutAct_9fa48("497") ? /\bAND\s+1\S*=\s*0\b/i : stryMutAct_9fa48("496") ? /\bAND\s+1\s=\s*0\b/i : stryMutAct_9fa48("495") ? /\bAND\S+1\s*=\s*0\b/i : stryMutAct_9fa48("494") ? /\bAND\s1\s*=\s*0\b/i : (stryCov_9fa48("494", "495", "496", "497", "498", "499"), /\bAND\s+1\s*=\s*0\b/i), // AND 1=0 (always false, used to bypass)
stryMutAct_9fa48("505") ? /\b1\s*=\s*1\S+(OR|AND)\b/i : stryMutAct_9fa48("504") ? /\b1\s*=\s*1\s(OR|AND)\b/i : stryMutAct_9fa48("503") ? /\b1\s*=\S*1\s+(OR|AND)\b/i : stryMutAct_9fa48("502") ? /\b1\s*=\s1\s+(OR|AND)\b/i : stryMutAct_9fa48("501") ? /\b1\S*=\s*1\s+(OR|AND)\b/i : stryMutAct_9fa48("500") ? /\b1\s=\s*1\s+(OR|AND)\b/i : (stryCov_9fa48("500", "501", "502", "503", "504", "505"), /\b1\s*=\s*1\s+(OR|AND)\b/i), // 1=1 OR/AND
stryMutAct_9fa48("509") ? /\b'\s*=\S*'/ : stryMutAct_9fa48("508") ? /\b'\s*=\s'/ : stryMutAct_9fa48("507") ? /\b'\S*=\s*'/ : stryMutAct_9fa48("506") ? /\b'\s=\s*'/ : (stryCov_9fa48("506", "507", "508", "509"), /\b'\s*=\s*'/), // '=' (empty string comparison trick)
// File operations
stryMutAct_9fa48("511") ? /INTO\S+OUTFILE/i : stryMutAct_9fa48("510") ? /INTO\sOUTFILE/i : (stryCov_9fa48("510", "511"), /INTO\s+OUTFILE/i),
// File write injection
/LOAD_FILE/i,
// File read injection

// SQL Server specific
/xp_cmdshell/i,
// SQL Server command execution
/sp_executesql/i, // SQL Server dynamic execution
// Comment-based bypass attempts
stryMutAct_9fa48("512") ? /\/\*.\*\// : (stryCov_9fa48("512"), /\/\*.*\*\//), // Inline comments used to obfuscate
stryMutAct_9fa48("515") ? /--\S*$/m : stryMutAct_9fa48("514") ? /--\s$/m : stryMutAct_9fa48("513") ? /--\s*/m : (stryCov_9fa48("513", "514", "515"), /--\s*$/m) // Line-ending comments
]);

/**
 * Check if a string contains dangerous SQL patterns
 * @returns {boolean} true if dangerous patterns found
 */
export function containsDangerousPatterns(str) {
  if (stryMutAct_9fa48("516")) {
    {}
  } else {
    stryCov_9fa48("516");
    if (stryMutAct_9fa48("519") ? typeof str === 'string' : stryMutAct_9fa48("518") ? false : stryMutAct_9fa48("517") ? true : (stryCov_9fa48("517", "518", "519"), typeof str !== 'string')) return stryMutAct_9fa48("521") ? true : (stryCov_9fa48("521"), false);
    return stryMutAct_9fa48("522") ? DANGEROUS_PATTERNS.every(pattern => pattern.test(str)) : (stryCov_9fa48("522"), DANGEROUS_PATTERNS.some(stryMutAct_9fa48("523") ? () => undefined : (stryCov_9fa48("523"), pattern => pattern.test(str))));
  }
}

/**
 * Escape a string for safe use in SQL LIKE patterns
 * Escapes: single quotes, backslashes, and LIKE wildcards (%, _)
 */
export function escapeLikePattern(str) {
  if (stryMutAct_9fa48("524")) {
    {}
  } else {
    stryCov_9fa48("524");
    if (stryMutAct_9fa48("527") ? typeof str === 'string' : stryMutAct_9fa48("526") ? false : stryMutAct_9fa48("525") ? true : (stryCov_9fa48("525", "526", "527"), typeof str !== 'string')) return '';
    if (stryMutAct_9fa48("531") ? false : stryMutAct_9fa48("530") ? true : (stryCov_9fa48("530", "531"), containsDangerousPatterns(str))) return '';
    return str.replace(/\\/g, '\\\\') // Escape backslashes first
    .replace(/'/g, "''") // Escape single quotes
    .replace(/%/g, '\\%') // Escape LIKE wildcard %
    .replace(/_/g, '\\_'); // Escape LIKE wildcard _
  }
}

/**
 * Escape a string for safe use in SQL string literals
 * Rejects dangerous patterns entirely, then escapes single quotes
 */
export function escapeString(str) {
  if (stryMutAct_9fa48("537")) {
    {}
  } else {
    stryCov_9fa48("537");
    if (stryMutAct_9fa48("540") ? typeof str === 'string' : stryMutAct_9fa48("539") ? false : stryMutAct_9fa48("538") ? true : (stryCov_9fa48("538", "539", "540"), typeof str !== 'string')) return '';
    if (stryMutAct_9fa48("544") ? false : stryMutAct_9fa48("543") ? true : (stryCov_9fa48("543", "544"), containsDangerousPatterns(str))) return '';
    return str.replace(/'/g, "''");
  }
}

/**
 * Validate and sanitize a numeric parameter
 * Returns the number if valid, otherwise returns the default value
 * Enforces reasonable bounds to prevent DoS attacks
 */
export function sanitizeNumber(value, {
  min = 0,
  max = 10000,
  defaultValue = 0
} = {}) {
  if (stryMutAct_9fa48("547")) {
    {}
  } else {
    stryCov_9fa48("547");
    const num = parseInt(value, 10);
    if (stryMutAct_9fa48("549") ? false : stryMutAct_9fa48("548") ? true : (stryCov_9fa48("548", "549"), isNaN(num))) return defaultValue;
    return stryMutAct_9fa48("550") ? Math.max(max, Math.max(min, num)) : (stryCov_9fa48("550"), Math.min(max, stryMutAct_9fa48("551") ? Math.min(min, num) : (stryCov_9fa48("551"), Math.max(min, num))));
  }
}

/**
 * Validate a string parameter against an allowed pattern
 * Rejects strings that don't match the pattern or contain dangerous SQL
 */
export function validatePattern(str, pattern, maxLength = 1000) {
  if (stryMutAct_9fa48("552")) {
    {}
  } else {
    stryCov_9fa48("552");
    if (stryMutAct_9fa48("555") ? typeof str === 'string' : stryMutAct_9fa48("554") ? false : stryMutAct_9fa48("553") ? true : (stryCov_9fa48("553", "554", "555"), typeof str !== 'string')) return null;
    if (stryMutAct_9fa48("560") ? str.length <= maxLength : stryMutAct_9fa48("559") ? str.length >= maxLength : stryMutAct_9fa48("558") ? false : stryMutAct_9fa48("557") ? true : (stryCov_9fa48("557", "558", "559", "560"), str.length > maxLength)) return null;
    if (stryMutAct_9fa48("562") ? false : stryMutAct_9fa48("561") ? true : (stryCov_9fa48("561", "562"), containsDangerousPatterns(str))) return null;
    if (stryMutAct_9fa48("565") ? false : stryMutAct_9fa48("564") ? true : stryMutAct_9fa48("563") ? pattern.test(str) : (stryCov_9fa48("563", "564", "565"), !pattern.test(str))) return null;
    return str;
  }
}

/**
 * Validate and sanitize an identifier (table name, column name, etc.)
 * Only allows alphanumeric, underscores, dots, and colons (for Daml template IDs)
 * Rejects any dangerous SQL patterns
 */
export function sanitizeIdentifier(str, maxLength = 500) {
  if (stryMutAct_9fa48("566")) {
    {}
  } else {
    stryCov_9fa48("566");
    if (stryMutAct_9fa48("568") ? false : stryMutAct_9fa48("567") ? true : (stryCov_9fa48("567", "568"), containsDangerousPatterns(str))) return null;
    return validatePattern(str, stryMutAct_9fa48("573") ? /^[\W.:@-]+$/i : stryMutAct_9fa48("572") ? /^[^\w.:@-]+$/i : stryMutAct_9fa48("571") ? /^[\w.:@-]$/i : stryMutAct_9fa48("570") ? /^[\w.:@-]+/i : stryMutAct_9fa48("569") ? /[\w.:@-]+$/i : (stryCov_9fa48("569", "570", "571", "572", "573"), /^[\w.:@-]+$/i), maxLength);
  }
}

/**
 * Validate a contract/event ID
 * Daml contract IDs format: 00hex::Package.Module:Template#suffix
 * Allows: hex chars, letters, numbers, colons, dots, dashes, hashes, underscores, at-signs
 */
export function sanitizeContractId(str) {
  if (stryMutAct_9fa48("574")) {
    {}
  } else {
    stryCov_9fa48("574");
    if (stryMutAct_9fa48("577") ? typeof str === 'string' : stryMutAct_9fa48("576") ? false : stryMutAct_9fa48("575") ? true : (stryCov_9fa48("575", "576", "577"), typeof str !== 'string')) return null;
    if (stryMutAct_9fa48("580") ? false : stryMutAct_9fa48("579") ? true : (stryCov_9fa48("579", "580"), containsDangerousPatterns(str))) return null;
    // Match Daml contract ID format: hex prefix, double colon, then template path
    // Examples: 00abc123::Splice.Amulet:Amulet, 00def456::Module:Template#0
    return validatePattern(str, stryMutAct_9fa48("593") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[^a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("592") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_])?$/ : stryMutAct_9fa48("591") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)$/ : stryMutAct_9fa48("590") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[^a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("589") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9])?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("588") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("587") ? /^[a-fA-F0-9]+(::[^a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("586") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-])?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("585") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("584") ? /^[^a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("583") ? /^[a-fA-F0-9](::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("582") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?/ : stryMutAct_9fa48("581") ? /[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : (stryCov_9fa48("581", "582", "583", "584", "585", "586", "587", "588", "589", "590", "591", "592", "593"), /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/), 500);
  }
}

/**
 * Validate an event type (created, archived, etc.)
 * Uses whitelist approach - only allows known values
 */
export function sanitizeEventType(str) {
  if (stryMutAct_9fa48("594")) {
    {}
  } else {
    stryCov_9fa48("594");
    const allowed = stryMutAct_9fa48("595") ? [] : (stryCov_9fa48("595"), ['created', 'archived', 'exercised', 'CreatedEvent', 'ArchivedEvent', 'ExercisedEvent']);
    if (stryMutAct_9fa48("603") ? false : stryMutAct_9fa48("602") ? true : (stryCov_9fa48("602", "603"), allowed.includes(str))) return str;
    return null;
  }
}

/**
 * Validate an ISO date/timestamp string
 * Rejects any non-date patterns to prevent injection
 */
export function sanitizeTimestamp(str) {
  if (stryMutAct_9fa48("604")) {
    {}
  } else {
    stryCov_9fa48("604");
    if (stryMutAct_9fa48("607") ? typeof str === 'string' : stryMutAct_9fa48("606") ? false : stryMutAct_9fa48("605") ? true : (stryCov_9fa48("605", "606", "607"), typeof str !== 'string')) return null;
    if (stryMutAct_9fa48("612") ? str.length <= 50 : stryMutAct_9fa48("611") ? str.length >= 50 : stryMutAct_9fa48("610") ? false : stryMutAct_9fa48("609") ? true : (stryCov_9fa48("609", "610", "611", "612"), str.length > 50)) return null; // Reasonable max length for timestamps
    if (stryMutAct_9fa48("614") ? false : stryMutAct_9fa48("613") ? true : (stryCov_9fa48("613", "614"), containsDangerousPatterns(str))) return null;

    // ISO 8601 format: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD
    const isoPattern = stryMutAct_9fa48("639") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\D{2})?)?$/ : stryMutAct_9fa48("638") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d)?)?$/ : stryMutAct_9fa48("637") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\D{2}:\d{2})?)?$/ : stryMutAct_9fa48("636") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d:\d{2})?)?$/ : stryMutAct_9fa48("635") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[^+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("634") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2}))?$/ : stryMutAct_9fa48("633") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\D+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("632") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("631") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("630") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\D{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("629") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d)?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("628") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("627") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\D{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("626") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("625") ? /^\d{4}-\d{2}-\d{2}(T\D{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("624") ? /^\d{4}-\d{2}-\d{2}(T\d:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("623") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)$/ : stryMutAct_9fa48("622") ? /^\d{4}-\d{2}-\D{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("621") ? /^\d{4}-\d{2}-\d(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("620") ? /^\d{4}-\D{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("619") ? /^\d{4}-\d-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("618") ? /^\D{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("617") ? /^\d-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("616") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?/ : stryMutAct_9fa48("615") ? /\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : (stryCov_9fa48("615", "616", "617", "618", "619", "620", "621", "622", "623", "624", "625", "626", "627", "628", "629", "630", "631", "632", "633", "634", "635", "636", "637", "638", "639"), /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/);
    if (stryMutAct_9fa48("642") ? false : stryMutAct_9fa48("641") ? true : stryMutAct_9fa48("640") ? isoPattern.test(str) : (stryCov_9fa48("640", "641", "642"), !isoPattern.test(str))) return null;

    // Verify it's a valid date using Date.parse
    const parsed = Date.parse(str);
    if (stryMutAct_9fa48("644") ? false : stryMutAct_9fa48("643") ? true : (stryCov_9fa48("643", "644"), isNaN(parsed))) return null;
    return str;
  }
}

/**
 * Validate a search query string
 * Allows alphanumeric, spaces, and common punctuation, but rejects SQL patterns
 */
export function sanitizeSearchQuery(str, maxLength = 200) {
  if (stryMutAct_9fa48("645")) {
    {}
  } else {
    stryCov_9fa48("645");
    if (stryMutAct_9fa48("648") ? typeof str === 'string' : stryMutAct_9fa48("647") ? false : stryMutAct_9fa48("646") ? true : (stryCov_9fa48("646", "647", "648"), typeof str !== 'string')) return null;
    if (stryMutAct_9fa48("653") ? str.length <= maxLength : stryMutAct_9fa48("652") ? str.length >= maxLength : stryMutAct_9fa48("651") ? false : stryMutAct_9fa48("650") ? true : (stryCov_9fa48("650", "651", "652", "653"), str.length > maxLength)) return null;
    if (stryMutAct_9fa48("655") ? false : stryMutAct_9fa48("654") ? true : (stryCov_9fa48("654", "655"), containsDangerousPatterns(str))) return null;

    // Allow letters, numbers, spaces, and limited punctuation
    // Reject anything that looks like SQL control characters
    if (stryMutAct_9fa48("657") ? false : stryMutAct_9fa48("656") ? true : (stryCov_9fa48("656", "657"), (stryMutAct_9fa48("658") ? /[^;'"\\`]/ : (stryCov_9fa48("658"), /[;'"\\`]/)).test(str))) return null;
    return stryMutAct_9fa48("659") ? str : (stryCov_9fa48("659"), str.trim());
  }
}

/**
 * Build a safe WHERE condition for LIKE queries
 * Returns null if the value contains dangerous patterns
 */
export function buildLikeCondition(column, value, position = 'contains') {
  if (stryMutAct_9fa48("661")) {
    {}
  } else {
    stryCov_9fa48("661");
    if (stryMutAct_9fa48("663") ? false : stryMutAct_9fa48("662") ? true : (stryCov_9fa48("662", "663"), containsDangerousPatterns(value))) return null;
    const escaped = escapeLikePattern(value);
    if (stryMutAct_9fa48("666") ? false : stryMutAct_9fa48("665") ? true : stryMutAct_9fa48("664") ? escaped : (stryCov_9fa48("664", "665", "666"), !escaped)) return null;
    switch (position) {
      case 'starts':
        if (stryMutAct_9fa48("667")) {} else {
          stryCov_9fa48("667");
          return `${column} LIKE '${escaped}%' ESCAPE '\\'`;
        }
      case 'ends':
        if (stryMutAct_9fa48("670")) {} else {
          stryCov_9fa48("670");
          return `${column} LIKE '%${escaped}' ESCAPE '\\'`;
        }
      case 'contains':
      default:
        if (stryMutAct_9fa48("674")) {} else {
          stryCov_9fa48("674");
          return `${column} LIKE '%${escaped}%' ESCAPE '\\'`;
        }
    }
  }
}

/**
 * Build a safe equality condition
 * Returns null if the value contains dangerous patterns
 */
export function buildEqualCondition(column, value) {
  if (stryMutAct_9fa48("676")) {
    {}
  } else {
    stryCov_9fa48("676");
    if (stryMutAct_9fa48("678") ? false : stryMutAct_9fa48("677") ? true : (stryCov_9fa48("677", "678"), containsDangerousPatterns(value))) return null;
    const escaped = escapeString(value);
    if (stryMutAct_9fa48("681") ? !escaped || value : stryMutAct_9fa48("680") ? false : stryMutAct_9fa48("679") ? true : (stryCov_9fa48("679", "680", "681"), (stryMutAct_9fa48("682") ? escaped : (stryCov_9fa48("682"), !escaped)) && value)) return null; // Value was rejected
    return `${column} = '${escaped}'`;
  }
}

/**
 * Validate and build a list condition (for IN clauses)
 * Rejects any values containing dangerous patterns
 */
export function buildInCondition(column, values, validator = escapeString) {
  if (stryMutAct_9fa48("684")) {
    {}
  } else {
    stryCov_9fa48("684");
    if (stryMutAct_9fa48("687") ? !Array.isArray(values) && values.length === 0 : stryMutAct_9fa48("686") ? false : stryMutAct_9fa48("685") ? true : (stryCov_9fa48("685", "686", "687"), (stryMutAct_9fa48("688") ? Array.isArray(values) : (stryCov_9fa48("688"), !Array.isArray(values))) || (stryMutAct_9fa48("690") ? values.length !== 0 : stryMutAct_9fa48("689") ? false : (stryCov_9fa48("689", "690"), values.length === 0)))) return null;
    const sanitized = stryMutAct_9fa48("692") ? values.map(v => validator(v)).filter(v => v !== null && v !== '') : stryMutAct_9fa48("691") ? values.filter(v => !containsDangerousPatterns(v)).map(v => validator(v)) : (stryCov_9fa48("691", "692"), values.filter(stryMutAct_9fa48("693") ? () => undefined : (stryCov_9fa48("693"), v => stryMutAct_9fa48("694") ? containsDangerousPatterns(v) : (stryCov_9fa48("694"), !containsDangerousPatterns(v)))).map(stryMutAct_9fa48("695") ? () => undefined : (stryCov_9fa48("695"), v => validator(v))).filter(stryMutAct_9fa48("696") ? () => undefined : (stryCov_9fa48("696"), v => stryMutAct_9fa48("699") ? v !== null || v !== '' : stryMutAct_9fa48("698") ? false : stryMutAct_9fa48("697") ? true : (stryCov_9fa48("697", "698", "699"), (stryMutAct_9fa48("701") ? v === null : stryMutAct_9fa48("700") ? true : (stryCov_9fa48("700", "701"), v !== null)) && (stryMutAct_9fa48("703") ? v === '' : stryMutAct_9fa48("702") ? true : (stryCov_9fa48("702", "703"), v !== ''))))));
    if (stryMutAct_9fa48("707") ? sanitized.length !== 0 : stryMutAct_9fa48("706") ? false : stryMutAct_9fa48("705") ? true : (stryCov_9fa48("705", "706", "707"), sanitized.length === 0)) return null;
    return `${column} IN (${sanitized.map(stryMutAct_9fa48("709") ? () => undefined : (stryCov_9fa48("709"), v => `'${v}'`)).join(', ')})`;
  }
}
export default {
  containsDangerousPatterns,
  escapeLikePattern,
  escapeString,
  sanitizeNumber,
  validatePattern,
  sanitizeIdentifier,
  sanitizeContractId,
  sanitizeEventType,
  sanitizeTimestamp,
  sanitizeSearchQuery,
  buildLikeCondition,
  buildEqualCondition,
  buildInCondition
};