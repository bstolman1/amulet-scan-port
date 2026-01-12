/**
 * Supabase Client Stub
 * 
 * NOTE: This project no longer uses Supabase for ledger data.
 * All ledger queries go through the DuckDB API server.
 * 
 * This stub exists only to satisfy TypeScript imports from the
 * auto-generated types.ts file (which is read-only).
 * 
 * DO NOT USE THIS CLIENT - it is not functional.
 */

// Minimal stub to prevent build errors from types.ts
export const supabase = null as unknown as {
  from: () => never;
  auth: { getSession: () => never };
};

console.warn(
  '[DEPRECATED] supabase client imported but Supabase is not used for ledger data. Use the DuckDB API instead.'
);
