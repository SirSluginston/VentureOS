/**
 * VentureOS Unified Data Schema (Core + Variant Pattern)
 * 
 * Strategy:
 * 1. CORE: Top-level columns for universal fields (Indexable, Fast).
 * 2. VARIANT: 'violation_details' (JSON) for domain-specific data.
 * 
 * This allows infinite extensibility (OSHA, FAA, EPA) without schema migrations.
 */

export const UNIFIED_SCHEMA = {
  // --- Core Identity (Universal) ---
  violation_id: 'VARCHAR',        // Global ID (e.g., "OSHA-2024-001")
  agency: 'VARCHAR',              // Partition Key (OSHA, FAA, EPA)
  
  // --- Location (Universal) ---
  state: 'VARCHAR',               // 2-letter code
  city: 'VARCHAR',                // Normalized Title Case
  
  // --- Company/Entity (Universal) ---
  company_name: 'VARCHAR',        // Normalized Name
  company_slug: 'VARCHAR',        // URL Slug
  
  // --- Event Stats (Universal) ---
  event_date: 'DATE',             // Primary Time Index
  fine_amount: 'DOUBLE',          // Financial Impact
  violation_type: 'VARCHAR',      // Severity Class (Serious, Critical)
  
  // --- Content (Universal) ---
  raw_title: 'VARCHAR',           // Headline
  raw_description: 'VARCHAR',     // Full Text
  bedrock_title: 'VARCHAR',       // AI Summary Title
  bedrock_description: 'VARCHAR',  // AI Summary Text
  bedrock_tags: 'VARCHAR[]',       // AI-generated tags
  bedrock_generated_at: 'TIMESTAMP', // When Bedrock generated content
  is_verified: 'BOOLEAN',         // Editorially reviewed?
  verified_at: 'TIMESTAMP',        // When verified
  verified_by: 'VARCHAR',          // Who verified (admin email)
  source_url: 'VARCHAR',          // Link to truth
  
  // --- Metadata ---
  tags: 'VARCHAR[]',              // Universal tagging system
  
  // --- The "Variant" Column ---
  // Stores domain-specific data as a JSON string.
  // DuckDB/Athena can query inside this efficiently.
  violation_details: 'JSON',      
};

/**
 * Helper to get DuckDB Schema
 */
export function getDuckDBSchema() {
  return UNIFIED_SCHEMA;
}

/**
 * Validate and Pack Row
 * Moves unknown fields into 'violation_details' automatically.
 */
export function validateRow(row) {
  const cleanRow = {};
  const details = {}; // bucket for extra fields

  // 1. Extract Core Fields
  for (const [key, type] of Object.entries(UNIFIED_SCHEMA)) {
    if (key === 'violation_details') continue;
    
    cleanRow[key] = row[key] !== undefined ? row[key] : null;
  }

  // 2. Move everything else to details
  // If the normalizer already provided 'violation_details', use it
  if (row.violation_details) {
    Object.assign(details, row.violation_details);
  }
  
  // Also catch any loose fields that aren't in schema
  for (const [key, val] of Object.entries(row)) {
    if (!UNIFIED_SCHEMA[key] && key !== 'violation_details') {
      details[key] = val;
    }
  }

  // 3. Stringify details for storage
  // We store as JSON string because Parquet doesn't have a native "JSON" type,
  // but DuckDB handles 'JSON' type by treating it as a specialized string.
  cleanRow.violation_details = JSON.stringify(details);

  return cleanRow;
}
