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
const DANGEROUS_PATTERNS = stryMutAct_9fa48("0") ? [] : (stryCov_9fa48("0"), [// Statement injection patterns
stryMutAct_9fa48("2") ? /;\S*--/i : stryMutAct_9fa48("1") ? /;\s--/i : (stryCov_9fa48("1", "2"), /;\s*--/i), // Comment after statement terminator
stryMutAct_9fa48("4") ? /;\S*\/\*/i : stryMutAct_9fa48("3") ? /;\s\/\*/i : (stryCov_9fa48("3", "4"), /;\s*\/\*/i), // Block comment after statement terminator
stryMutAct_9fa48("8") ? /'\s*;\S*DROP/i : stryMutAct_9fa48("7") ? /'\s*;\sDROP/i : stryMutAct_9fa48("6") ? /'\S*;\s*DROP/i : stryMutAct_9fa48("5") ? /'\s;\s*DROP/i : (stryCov_9fa48("5", "6", "7", "8"), /'\s*;\s*DROP/i), // DROP statement injection
stryMutAct_9fa48("12") ? /'\s*;\S*DELETE/i : stryMutAct_9fa48("11") ? /'\s*;\sDELETE/i : stryMutAct_9fa48("10") ? /'\S*;\s*DELETE/i : stryMutAct_9fa48("9") ? /'\s;\s*DELETE/i : (stryCov_9fa48("9", "10", "11", "12"), /'\s*;\s*DELETE/i), // DELETE statement injection
stryMutAct_9fa48("16") ? /'\s*;\S*UPDATE/i : stryMutAct_9fa48("15") ? /'\s*;\sUPDATE/i : stryMutAct_9fa48("14") ? /'\S*;\s*UPDATE/i : stryMutAct_9fa48("13") ? /'\s;\s*UPDATE/i : (stryCov_9fa48("13", "14", "15", "16"), /'\s*;\s*UPDATE/i), // UPDATE statement injection
stryMutAct_9fa48("20") ? /'\s*;\S*INSERT/i : stryMutAct_9fa48("19") ? /'\s*;\sINSERT/i : stryMutAct_9fa48("18") ? /'\S*;\s*INSERT/i : stryMutAct_9fa48("17") ? /'\s;\s*INSERT/i : (stryCov_9fa48("17", "18", "19", "20"), /'\s*;\s*INSERT/i), // INSERT statement injection
stryMutAct_9fa48("24") ? /'\s*;\S*ALTER/i : stryMutAct_9fa48("23") ? /'\s*;\sALTER/i : stryMutAct_9fa48("22") ? /'\S*;\s*ALTER/i : stryMutAct_9fa48("21") ? /'\s;\s*ALTER/i : (stryCov_9fa48("21", "22", "23", "24"), /'\s*;\s*ALTER/i), // ALTER statement injection
stryMutAct_9fa48("28") ? /'\s*;\S*CREATE/i : stryMutAct_9fa48("27") ? /'\s*;\sCREATE/i : stryMutAct_9fa48("26") ? /'\S*;\s*CREATE/i : stryMutAct_9fa48("25") ? /'\s;\s*CREATE/i : (stryCov_9fa48("25", "26", "27", "28"), /'\s*;\s*CREATE/i), // CREATE statement injection
stryMutAct_9fa48("32") ? /'\s*;\S*TRUNCATE/i : stryMutAct_9fa48("31") ? /'\s*;\sTRUNCATE/i : stryMutAct_9fa48("30") ? /'\S*;\s*TRUNCATE/i : stryMutAct_9fa48("29") ? /'\s;\s*TRUNCATE/i : (stryCov_9fa48("29", "30", "31", "32"), /'\s*;\s*TRUNCATE/i), // TRUNCATE statement injection
stryMutAct_9fa48("36") ? /'\s*;\S*EXEC/i : stryMutAct_9fa48("35") ? /'\s*;\sEXEC/i : stryMutAct_9fa48("34") ? /'\S*;\s*EXEC/i : stryMutAct_9fa48("33") ? /'\s;\s*EXEC/i : (stryCov_9fa48("33", "34", "35", "36"), /'\s*;\s*EXEC/i), // EXEC statement injection
// UNION-based injection
stryMutAct_9fa48("38") ? /UNION\S+SELECT/i : stryMutAct_9fa48("37") ? /UNION\sSELECT/i : (stryCov_9fa48("37", "38"), /UNION\s+SELECT/i), // UNION injection
stryMutAct_9fa48("40") ? /UNION\S+ALL/i : stryMutAct_9fa48("39") ? /UNION\sALL/i : (stryCov_9fa48("39", "40"), /UNION\s+ALL/i), // UNION ALL injection
// Tautology-based injection (always-true conditions)
stryMutAct_9fa48("52") ? /'\s*OR\s+'[^']*'\s*=\s*'[']*'/i : stryMutAct_9fa48("51") ? /'\s*OR\s+'[^']*'\s*=\s*'[^']'/i : stryMutAct_9fa48("50") ? /'\s*OR\s+'[^']*'\s*=\S*'[^']*'/i : stryMutAct_9fa48("49") ? /'\s*OR\s+'[^']*'\s*=\s'[^']*'/i : stryMutAct_9fa48("48") ? /'\s*OR\s+'[^']*'\S*=\s*'[^']*'/i : stryMutAct_9fa48("47") ? /'\s*OR\s+'[^']*'\s=\s*'[^']*'/i : stryMutAct_9fa48("46") ? /'\s*OR\s+'[']*'\s*=\s*'[^']*'/i : stryMutAct_9fa48("45") ? /'\s*OR\s+'[^']'\s*=\s*'[^']*'/i : stryMutAct_9fa48("44") ? /'\s*OR\S+'[^']*'\s*=\s*'[^']*'/i : stryMutAct_9fa48("43") ? /'\s*OR\s'[^']*'\s*=\s*'[^']*'/i : stryMutAct_9fa48("42") ? /'\S*OR\s+'[^']*'\s*=\s*'[^']*'/i : stryMutAct_9fa48("41") ? /'\sOR\s+'[^']*'\s*=\s*'[^']*'/i : (stryCov_9fa48("41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52"), /'\s*OR\s+'[^']*'\s*=\s*'[^']*'/i), // 'x' OR 'a'='a'
stryMutAct_9fa48("64") ? /'\s*OR\s+\d+\s*=\s*\D+/i : stryMutAct_9fa48("63") ? /'\s*OR\s+\d+\s*=\s*\d/i : stryMutAct_9fa48("62") ? /'\s*OR\s+\d+\s*=\S*\d+/i : stryMutAct_9fa48("61") ? /'\s*OR\s+\d+\s*=\s\d+/i : stryMutAct_9fa48("60") ? /'\s*OR\s+\d+\S*=\s*\d+/i : stryMutAct_9fa48("59") ? /'\s*OR\s+\d+\s=\s*\d+/i : stryMutAct_9fa48("58") ? /'\s*OR\s+\D+\s*=\s*\d+/i : stryMutAct_9fa48("57") ? /'\s*OR\s+\d\s*=\s*\d+/i : stryMutAct_9fa48("56") ? /'\s*OR\S+\d+\s*=\s*\d+/i : stryMutAct_9fa48("55") ? /'\s*OR\s\d+\s*=\s*\d+/i : stryMutAct_9fa48("54") ? /'\S*OR\s+\d+\s*=\s*\d+/i : stryMutAct_9fa48("53") ? /'\sOR\s+\d+\s*=\s*\d+/i : (stryCov_9fa48("53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64"), /'\s*OR\s+\d+\s*=\s*\d+/i), // 'x' OR 1=1
stryMutAct_9fa48("70") ? /\bOR\s+1\s*=\S*1\b/i : stryMutAct_9fa48("69") ? /\bOR\s+1\s*=\s1\b/i : stryMutAct_9fa48("68") ? /\bOR\s+1\S*=\s*1\b/i : stryMutAct_9fa48("67") ? /\bOR\s+1\s=\s*1\b/i : stryMutAct_9fa48("66") ? /\bOR\S+1\s*=\s*1\b/i : stryMutAct_9fa48("65") ? /\bOR\s1\s*=\s*1\b/i : (stryCov_9fa48("65", "66", "67", "68", "69", "70"), /\bOR\s+1\s*=\s*1\b/i), // OR 1=1
stryMutAct_9fa48("78") ? /\bOR\s+'.*'\s*=\s*'.'/i : stryMutAct_9fa48("77") ? /\bOR\s+'.*'\s*=\S*'.*'/i : stryMutAct_9fa48("76") ? /\bOR\s+'.*'\s*=\s'.*'/i : stryMutAct_9fa48("75") ? /\bOR\s+'.*'\S*=\s*'.*'/i : stryMutAct_9fa48("74") ? /\bOR\s+'.*'\s=\s*'.*'/i : stryMutAct_9fa48("73") ? /\bOR\s+'.'\s*=\s*'.*'/i : stryMutAct_9fa48("72") ? /\bOR\S+'.*'\s*=\s*'.*'/i : stryMutAct_9fa48("71") ? /\bOR\s'.*'\s*=\s*'.*'/i : (stryCov_9fa48("71", "72", "73", "74", "75", "76", "77", "78"), /\bOR\s+'.*'\s*=\s*'.*'/i), // OR 'a'='a'
stryMutAct_9fa48("80") ? /\bOR\S+true\b/i : stryMutAct_9fa48("79") ? /\bOR\strue\b/i : (stryCov_9fa48("79", "80"), /\bOR\s+true\b/i), // OR true
stryMutAct_9fa48("86") ? /\bAND\s+1\s*=\S*0\b/i : stryMutAct_9fa48("85") ? /\bAND\s+1\s*=\s0\b/i : stryMutAct_9fa48("84") ? /\bAND\s+1\S*=\s*0\b/i : stryMutAct_9fa48("83") ? /\bAND\s+1\s=\s*0\b/i : stryMutAct_9fa48("82") ? /\bAND\S+1\s*=\s*0\b/i : stryMutAct_9fa48("81") ? /\bAND\s1\s*=\s*0\b/i : (stryCov_9fa48("81", "82", "83", "84", "85", "86"), /\bAND\s+1\s*=\s*0\b/i), // AND 1=0 (always false, used to bypass)
stryMutAct_9fa48("92") ? /\b1\s*=\s*1\S+(OR|AND)\b/i : stryMutAct_9fa48("91") ? /\b1\s*=\s*1\s(OR|AND)\b/i : stryMutAct_9fa48("90") ? /\b1\s*=\S*1\s+(OR|AND)\b/i : stryMutAct_9fa48("89") ? /\b1\s*=\s1\s+(OR|AND)\b/i : stryMutAct_9fa48("88") ? /\b1\S*=\s*1\s+(OR|AND)\b/i : stryMutAct_9fa48("87") ? /\b1\s=\s*1\s+(OR|AND)\b/i : (stryCov_9fa48("87", "88", "89", "90", "91", "92"), /\b1\s*=\s*1\s+(OR|AND)\b/i), // 1=1 OR/AND
stryMutAct_9fa48("96") ? /\b'\s*=\S*'/ : stryMutAct_9fa48("95") ? /\b'\s*=\s'/ : stryMutAct_9fa48("94") ? /\b'\S*=\s*'/ : stryMutAct_9fa48("93") ? /\b'\s=\s*'/ : (stryCov_9fa48("93", "94", "95", "96"), /\b'\s*=\s*'/), // '=' (empty string comparison trick)
// File operations
stryMutAct_9fa48("98") ? /INTO\S+OUTFILE/i : stryMutAct_9fa48("97") ? /INTO\sOUTFILE/i : (stryCov_9fa48("97", "98"), /INTO\s+OUTFILE/i),
// File write injection
/LOAD_FILE/i,
// File read injection

// SQL Server specific
/xp_cmdshell/i,
// SQL Server command execution
/sp_executesql/i, // SQL Server dynamic execution
// Comment-based bypass attempts
stryMutAct_9fa48("99") ? /\/\*.\*\// : (stryCov_9fa48("99"), /\/\*.*\*\//), // Inline comments used to obfuscate
stryMutAct_9fa48("102") ? /--\S*$/m : stryMutAct_9fa48("101") ? /--\s$/m : stryMutAct_9fa48("100") ? /--\s*/m : (stryCov_9fa48("100", "101", "102"), /--\s*$/m) // Line-ending comments
]);

/**
 * Check if a string contains dangerous SQL patterns
 * @returns {boolean} true if dangerous patterns found
 */
export function containsDangerousPatterns(str) {
  if (stryMutAct_9fa48("103")) {
    {}
  } else {
    stryCov_9fa48("103");
    if (stryMutAct_9fa48("106") ? typeof str === 'string' : stryMutAct_9fa48("105") ? false : stryMutAct_9fa48("104") ? true : (stryCov_9fa48("104", "105", "106"), typeof str !== 'string')) return stryMutAct_9fa48("108") ? true : (stryCov_9fa48("108"), false);
    return stryMutAct_9fa48("109") ? DANGEROUS_PATTERNS.every(pattern => pattern.test(str)) : (stryCov_9fa48("109"), DANGEROUS_PATTERNS.some(stryMutAct_9fa48("110") ? () => undefined : (stryCov_9fa48("110"), pattern => pattern.test(str))));
  }
}

/**
 * Escape a string for safe use in SQL LIKE patterns
 * Escapes: single quotes, backslashes, and LIKE wildcards (%, _)
 */
export function escapeLikePattern(str) {
  if (stryMutAct_9fa48("111")) {
    {}
  } else {
    stryCov_9fa48("111");
    if (stryMutAct_9fa48("114") ? typeof str === 'string' : stryMutAct_9fa48("113") ? false : stryMutAct_9fa48("112") ? true : (stryCov_9fa48("112", "113", "114"), typeof str !== 'string')) return '';
    if (stryMutAct_9fa48("118") ? false : stryMutAct_9fa48("117") ? true : (stryCov_9fa48("117", "118"), containsDangerousPatterns(str))) return '';
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
  if (stryMutAct_9fa48("124")) {
    {}
  } else {
    stryCov_9fa48("124");
    if (stryMutAct_9fa48("127") ? typeof str === 'string' : stryMutAct_9fa48("126") ? false : stryMutAct_9fa48("125") ? true : (stryCov_9fa48("125", "126", "127"), typeof str !== 'string')) return '';
    if (stryMutAct_9fa48("131") ? false : stryMutAct_9fa48("130") ? true : (stryCov_9fa48("130", "131"), containsDangerousPatterns(str))) return '';
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
  if (stryMutAct_9fa48("134")) {
    {}
  } else {
    stryCov_9fa48("134");
    const num = parseInt(value, 10);
    if (stryMutAct_9fa48("136") ? false : stryMutAct_9fa48("135") ? true : (stryCov_9fa48("135", "136"), isNaN(num))) return defaultValue;
    return stryMutAct_9fa48("137") ? Math.max(max, Math.max(min, num)) : (stryCov_9fa48("137"), Math.min(max, stryMutAct_9fa48("138") ? Math.min(min, num) : (stryCov_9fa48("138"), Math.max(min, num))));
  }
}

/**
 * Validate a string parameter against an allowed pattern
 * Rejects strings that don't match the pattern or contain dangerous SQL
 */
export function validatePattern(str, pattern, maxLength = 1000) {
  if (stryMutAct_9fa48("139")) {
    {}
  } else {
    stryCov_9fa48("139");
    if (stryMutAct_9fa48("142") ? typeof str === 'string' : stryMutAct_9fa48("141") ? false : stryMutAct_9fa48("140") ? true : (stryCov_9fa48("140", "141", "142"), typeof str !== 'string')) return null;
    if (stryMutAct_9fa48("147") ? str.length <= maxLength : stryMutAct_9fa48("146") ? str.length >= maxLength : stryMutAct_9fa48("145") ? false : stryMutAct_9fa48("144") ? true : (stryCov_9fa48("144", "145", "146", "147"), str.length > maxLength)) return null;
    if (stryMutAct_9fa48("149") ? false : stryMutAct_9fa48("148") ? true : (stryCov_9fa48("148", "149"), containsDangerousPatterns(str))) return null;
    if (stryMutAct_9fa48("152") ? false : stryMutAct_9fa48("151") ? true : stryMutAct_9fa48("150") ? pattern.test(str) : (stryCov_9fa48("150", "151", "152"), !pattern.test(str))) return null;
    return str;
  }
}

/**
 * Validate and sanitize an identifier (table name, column name, etc.)
 * Only allows alphanumeric, underscores, dots, and colons (for Daml template IDs)
 * Rejects any dangerous SQL patterns
 */
export function sanitizeIdentifier(str, maxLength = 500) {
  if (stryMutAct_9fa48("153")) {
    {}
  } else {
    stryCov_9fa48("153");
    if (stryMutAct_9fa48("155") ? false : stryMutAct_9fa48("154") ? true : (stryCov_9fa48("154", "155"), containsDangerousPatterns(str))) return null;
    return validatePattern(str, stryMutAct_9fa48("160") ? /^[\W.:@-]+$/i : stryMutAct_9fa48("159") ? /^[^\w.:@-]+$/i : stryMutAct_9fa48("158") ? /^[\w.:@-]$/i : stryMutAct_9fa48("157") ? /^[\w.:@-]+/i : stryMutAct_9fa48("156") ? /[\w.:@-]+$/i : (stryCov_9fa48("156", "157", "158", "159", "160"), /^[\w.:@-]+$/i), maxLength);
  }
}

/**
 * Validate a contract/event ID
 * Daml contract IDs format: 00hex::Package.Module:Template#suffix
 * Allows: hex chars, letters, numbers, colons, dots, dashes, hashes, underscores, at-signs
 */
export function sanitizeContractId(str) {
  if (stryMutAct_9fa48("161")) {
    {}
  } else {
    stryCov_9fa48("161");
    if (stryMutAct_9fa48("164") ? typeof str === 'string' : stryMutAct_9fa48("163") ? false : stryMutAct_9fa48("162") ? true : (stryCov_9fa48("162", "163", "164"), typeof str !== 'string')) return null;
    if (stryMutAct_9fa48("167") ? false : stryMutAct_9fa48("166") ? true : (stryCov_9fa48("166", "167"), containsDangerousPatterns(str))) return null;
    // Match Daml contract ID format: hex prefix, double colon, then template path
    // Examples: 00abc123::Splice.Amulet:Amulet, 00def456::Module:Template#0
    return validatePattern(str, stryMutAct_9fa48("180") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[^a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("179") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_])?$/ : stryMutAct_9fa48("178") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)$/ : stryMutAct_9fa48("177") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[^a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("176") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9])?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("175") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("174") ? /^[a-fA-F0-9]+(::[^a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("173") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-])?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("172") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("171") ? /^[^a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("170") ? /^[a-fA-F0-9](::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("169") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?/ : stryMutAct_9fa48("168") ? /[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : (stryCov_9fa48("168", "169", "170", "171", "172", "173", "174", "175", "176", "177", "178", "179", "180"), /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/), 500);
  }
}

/**
 * Validate an event type (created, archived, etc.)
 * Uses whitelist approach - only allows known values
 */
export function sanitizeEventType(str) {
  if (stryMutAct_9fa48("181")) {
    {}
  } else {
    stryCov_9fa48("181");
    const allowed = stryMutAct_9fa48("182") ? [] : (stryCov_9fa48("182"), ['created', 'archived', 'exercised', 'CreatedEvent', 'ArchivedEvent', 'ExercisedEvent']);
    if (stryMutAct_9fa48("190") ? false : stryMutAct_9fa48("189") ? true : (stryCov_9fa48("189", "190"), allowed.includes(str))) return str;
    return null;
  }
}

/**
 * Validate an ISO date/timestamp string
 * Rejects any non-date patterns to prevent injection
 */
export function sanitizeTimestamp(str) {
  if (stryMutAct_9fa48("191")) {
    {}
  } else {
    stryCov_9fa48("191");
    if (stryMutAct_9fa48("194") ? typeof str === 'string' : stryMutAct_9fa48("193") ? false : stryMutAct_9fa48("192") ? true : (stryCov_9fa48("192", "193", "194"), typeof str !== 'string')) return null;
    if (stryMutAct_9fa48("199") ? str.length <= 50 : stryMutAct_9fa48("198") ? str.length >= 50 : stryMutAct_9fa48("197") ? false : stryMutAct_9fa48("196") ? true : (stryCov_9fa48("196", "197", "198", "199"), str.length > 50)) return null; // Reasonable max length for timestamps
    if (stryMutAct_9fa48("201") ? false : stryMutAct_9fa48("200") ? true : (stryCov_9fa48("200", "201"), containsDangerousPatterns(str))) return null;

    // ISO 8601 format: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD
    const isoPattern = stryMutAct_9fa48("226") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\D{2})?)?$/ : stryMutAct_9fa48("225") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d)?)?$/ : stryMutAct_9fa48("224") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\D{2}:\d{2})?)?$/ : stryMutAct_9fa48("223") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d:\d{2})?)?$/ : stryMutAct_9fa48("222") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[^+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("221") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2}))?$/ : stryMutAct_9fa48("220") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\D+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("219") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("218") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("217") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\D{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("216") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d)?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("215") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("214") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\D{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("213") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("212") ? /^\d{4}-\d{2}-\d{2}(T\D{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("211") ? /^\d{4}-\d{2}-\d{2}(T\d:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("210") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)$/ : stryMutAct_9fa48("209") ? /^\d{4}-\d{2}-\D{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("208") ? /^\d{4}-\d{2}-\d(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("207") ? /^\d{4}-\D{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("206") ? /^\d{4}-\d-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("205") ? /^\D{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("204") ? /^\d-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("203") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?/ : stryMutAct_9fa48("202") ? /\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : (stryCov_9fa48("202", "203", "204", "205", "206", "207", "208", "209", "210", "211", "212", "213", "214", "215", "216", "217", "218", "219", "220", "221", "222", "223", "224", "225", "226"), /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/);
    if (stryMutAct_9fa48("229") ? false : stryMutAct_9fa48("228") ? true : stryMutAct_9fa48("227") ? isoPattern.test(str) : (stryCov_9fa48("227", "228", "229"), !isoPattern.test(str))) return null;

    // Verify it's a valid date using Date.parse
    const parsed = Date.parse(str);
    if (stryMutAct_9fa48("231") ? false : stryMutAct_9fa48("230") ? true : (stryCov_9fa48("230", "231"), isNaN(parsed))) return null;
    return str;
  }
}

/**
 * Validate a search query string
 * Allows alphanumeric, spaces, and common punctuation, but rejects SQL patterns
 */
export function sanitizeSearchQuery(str, maxLength = 200) {
  if (stryMutAct_9fa48("232")) {
    {}
  } else {
    stryCov_9fa48("232");
    if (stryMutAct_9fa48("235") ? typeof str === 'string' : stryMutAct_9fa48("234") ? false : stryMutAct_9fa48("233") ? true : (stryCov_9fa48("233", "234", "235"), typeof str !== 'string')) return null;
    if (stryMutAct_9fa48("240") ? str.length <= maxLength : stryMutAct_9fa48("239") ? str.length >= maxLength : stryMutAct_9fa48("238") ? false : stryMutAct_9fa48("237") ? true : (stryCov_9fa48("237", "238", "239", "240"), str.length > maxLength)) return null;
    if (stryMutAct_9fa48("242") ? false : stryMutAct_9fa48("241") ? true : (stryCov_9fa48("241", "242"), containsDangerousPatterns(str))) return null;

    // Allow letters, numbers, spaces, and limited punctuation
    // Reject anything that looks like SQL control characters
    if (stryMutAct_9fa48("244") ? false : stryMutAct_9fa48("243") ? true : (stryCov_9fa48("243", "244"), (stryMutAct_9fa48("245") ? /[^;'"\\`]/ : (stryCov_9fa48("245"), /[;'"\\`]/)).test(str))) return null;
    return stryMutAct_9fa48("246") ? str : (stryCov_9fa48("246"), str.trim());
  }
}

/**
 * Build a safe WHERE condition for LIKE queries
 * Returns null if the value contains dangerous patterns
 */
export function buildLikeCondition(column, value, position = 'contains') {
  if (stryMutAct_9fa48("248")) {
    {}
  } else {
    stryCov_9fa48("248");
    if (stryMutAct_9fa48("250") ? false : stryMutAct_9fa48("249") ? true : (stryCov_9fa48("249", "250"), containsDangerousPatterns(value))) return null;
    const escaped = escapeLikePattern(value);
    if (stryMutAct_9fa48("253") ? false : stryMutAct_9fa48("252") ? true : stryMutAct_9fa48("251") ? escaped : (stryCov_9fa48("251", "252", "253"), !escaped)) return null;
    switch (position) {
      case 'starts':
        if (stryMutAct_9fa48("254")) {} else {
          stryCov_9fa48("254");
          return `${column} LIKE '${escaped}%' ESCAPE '\\'`;
        }
      case 'ends':
        if (stryMutAct_9fa48("257")) {} else {
          stryCov_9fa48("257");
          return `${column} LIKE '%${escaped}' ESCAPE '\\'`;
        }
      case 'contains':
      default:
        if (stryMutAct_9fa48("261")) {} else {
          stryCov_9fa48("261");
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
  if (stryMutAct_9fa48("263")) {
    {}
  } else {
    stryCov_9fa48("263");
    if (stryMutAct_9fa48("265") ? false : stryMutAct_9fa48("264") ? true : (stryCov_9fa48("264", "265"), containsDangerousPatterns(value))) return null;
    const escaped = escapeString(value);
    if (stryMutAct_9fa48("268") ? !escaped || value : stryMutAct_9fa48("267") ? false : stryMutAct_9fa48("266") ? true : (stryCov_9fa48("266", "267", "268"), (stryMutAct_9fa48("269") ? escaped : (stryCov_9fa48("269"), !escaped)) && value)) return null; // Value was rejected
    return `${column} = '${escaped}'`;
  }
}

/**
 * Validate and build a list condition (for IN clauses)
 * Rejects any values containing dangerous patterns
 */
export function buildInCondition(column, values, validator = escapeString) {
  if (stryMutAct_9fa48("271")) {
    {}
  } else {
    stryCov_9fa48("271");
    if (stryMutAct_9fa48("274") ? !Array.isArray(values) && values.length === 0 : stryMutAct_9fa48("273") ? false : stryMutAct_9fa48("272") ? true : (stryCov_9fa48("272", "273", "274"), (stryMutAct_9fa48("275") ? Array.isArray(values) : (stryCov_9fa48("275"), !Array.isArray(values))) || (stryMutAct_9fa48("277") ? values.length !== 0 : stryMutAct_9fa48("276") ? false : (stryCov_9fa48("276", "277"), values.length === 0)))) return null;
    const sanitized = stryMutAct_9fa48("279") ? values.map(v => validator(v)).filter(v => v !== null && v !== '') : stryMutAct_9fa48("278") ? values.filter(v => !containsDangerousPatterns(v)).map(v => validator(v)) : (stryCov_9fa48("278", "279"), values.filter(stryMutAct_9fa48("280") ? () => undefined : (stryCov_9fa48("280"), v => stryMutAct_9fa48("281") ? containsDangerousPatterns(v) : (stryCov_9fa48("281"), !containsDangerousPatterns(v)))).map(stryMutAct_9fa48("282") ? () => undefined : (stryCov_9fa48("282"), v => validator(v))).filter(stryMutAct_9fa48("283") ? () => undefined : (stryCov_9fa48("283"), v => stryMutAct_9fa48("286") ? v !== null || v !== '' : stryMutAct_9fa48("285") ? false : stryMutAct_9fa48("284") ? true : (stryCov_9fa48("284", "285", "286"), (stryMutAct_9fa48("288") ? v === null : stryMutAct_9fa48("287") ? true : (stryCov_9fa48("287", "288"), v !== null)) && (stryMutAct_9fa48("290") ? v === '' : stryMutAct_9fa48("289") ? true : (stryCov_9fa48("289", "290"), v !== ''))))));
    if (stryMutAct_9fa48("294") ? sanitized.length !== 0 : stryMutAct_9fa48("293") ? false : stryMutAct_9fa48("292") ? true : (stryCov_9fa48("292", "293", "294"), sanitized.length === 0)) return null;
    return `${column} IN (${sanitized.map(stryMutAct_9fa48("296") ? () => undefined : (stryCov_9fa48("296"), v => `'${v}'`)).join(', ')})`;
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