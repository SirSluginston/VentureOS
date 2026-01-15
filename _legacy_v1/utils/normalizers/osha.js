/**
 * OSHA Data Normalizer (VentureOS Strategy)
 * 
 * Maps raw OSHA CSV data (Severe Injury & Enforcement) to the Unified Schema.
 * Uses the "Variant" pattern for domain-specific fields.
 */

import { validateRow } from '../schema.js';

/**
 * Normalize OSHA Severe Injury Report data
 */
export function normalizeSevereInjuryReport(rawRow) {
  const eventDate = parseDate(rawRow.eventdate);
  const city = normalizeCity(rawRow.city);
  const state = normalizeState(rawRow.state);
  const employerName = rawRow.employer || '';
  
  const violationId = rawRow.id || generateViolationId({
    date: eventDate,
    establishment: employerName,
    city,
    state,
    upa: rawRow.upa,
  });

  const row = {
    // --- Core Identity ---
    violation_id: violationId,
    agency: 'OSHA',
    state: state,
    city: city,
    company_name: employerName,
    company_slug: slugify(employerName),
    event_date: eventDate,
    fine_amount: 0,
    violation_type: 'Severe Injury',
    raw_title: rawRow.event || rawRow.eventtitle || (rawRow.final_narrative ? rawRow.final_narrative.substring(0, 50) + '...' : `Safety Incident at ${employerName}`),
    raw_description: rawRow.final_narrative || '',
    source_url: null,
    tags: generateTags(rawRow),
    
    // --- Domain Specific (OSHA) ---
    // These will be automatically packed into 'violation_details' by validateRow
    osha_id: rawRow.id,
    upa_number: rawRow.upa,
    inspection_id: rawRow.inspection,
    zip_code: rawRow.zip,
    naics_code: rawRow.primary_naics,
    
    // Injury Specifics
    injury_nature: rawRow.nature || rawRow.naturetitle,
    body_part: rawRow.part_of_body || rawRow.part_of_body_title,
    injury_event: rawRow.event || rawRow.eventtitle,
    injury_source: rawRow.source || rawRow.sourcetitle,
    hospitalized: parseInt(rawRow.hospitalized) || 0,
    amputation: parseInt(rawRow.amputation) || 0,
    loss_of_eye: parseInt(rawRow.loss_of_eye) || 0
  };

  return validateRow(row);
}

/**
 * Normalize OSHA Enforcement Data
 */
export function normalizeEnforcementData(rawRow) {
  const inspectionDate = parseDate(rawRow.inspection_date || rawRow.inspectionDate);
  const city = normalizeCity(rawRow.city || rawRow.establishment_city);
  const state = normalizeState(rawRow.state || rawRow.establishment_state);
  const establishmentName = rawRow.establishment_name || rawRow.establishmentName || '';
  
  const violationId = generateViolationId({
    date: inspectionDate,
    establishment: establishmentName,
    city,
    state,
    citationId: rawRow.citation_id || rawRow.citationId,
  });

  const penalty = parseFloat(rawRow.penalty || rawRow.initial_penalty || 0);

  const row = {
    // --- Core Identity ---
    violation_id: violationId,
    agency: 'OSHA',
    state: state,
    city: city,
    company_name: establishmentName,
    company_slug: slugify(establishmentName),
    event_date: inspectionDate,
    fine_amount: penalty,
    violation_type: rawRow.violation_type || 'Enforcement',
    raw_title: rawRow.standard || rawRow.osha_standard || 'OSHA Violation',
    raw_description: rawRow.description || rawRow.violation_description || '',
    source_url: null,
    tags: ['enforcement', 'penalty'],
    
    // --- Domain Specific (OSHA) ---
    citation_id: rawRow.citation_id || rawRow.citationId,
    standard_id: rawRow.standard || rawRow.osha_standard,
    status: rawRow.status,
    naics_code: rawRow.naics,
    case_type: 'Enforcement'
  };

  return validateRow(row);
}

// --- Helpers ---

function parseDate(dateStr) {
  if (!dateStr) return null;
  if (dateStr.includes('/')) {
    const parts = dateStr.split('/');
    if (parts.length === 3) {
      const month = parseInt(parts[0], 10);
      const day = parseInt(parts[1], 10);
      const year = parseInt(parts[2], 10);
      const date = new Date(year, month - 1, day);
      if (!isNaN(date.getTime())) {
        return date.toISOString().split('T')[0];
      }
    }
  }
  const date = new Date(dateStr);
  if (isNaN(date.getTime())) return null;
  return date.toISOString().split('T')[0];
}

function normalizeCity(city) {
  if (!city) return '';
  return city.trim().split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
}

function normalizeState(state) {
  if (!state) return null;
  
  const cleanState = state.toLowerCase().trim();
  
  // Valid US States and Territories
  const validStates = new Set([
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', // District of Columbia
    'PR', 'VI', 'GU', 'AS', 'MP' // Territories
  ]);

  const stateMap = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
    'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
    'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
    'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
    'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
    'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
    'puerto rico': 'PR', 'virgin islands': 'VI', 'guam': 'GU', 
    'american samoa': 'AS', 'northern mariana islands': 'MP',
    // Common variants found in data
    'pu': 'PR', // Puerto Rico variant
    'am': 'AS', // American Samoa variant (Pago Pago)
    'cn': 'MP'  // Northern Mariana Islands variant
  };

  // 1. Try Map lookup
  let code = stateMap[cleanState];

  // 2. Fallback: If 2 letters, uppercase it
  if (!code && cleanState.length === 2) {
    code = cleanState.toUpperCase();
  }

  // 3. Validation: Only return if it's a known US State/Territory
  return validStates.has(code) ? code : null;
}

function slugify(str) {
  if (!str) return '';
  return str.toLowerCase().trim()
    .replace(/[^\w\s-]/g, '')
    .replace(/[\s_-]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function generateViolationId({ date, establishment, city, state, citationId, upa }) {
  const parts = [
    date || 'unknown',
    slugify(establishment).substring(0, 20),
    slugify(city).substring(0, 15),
    state,
    upa || citationId || Math.random().toString(36).substring(7),
  ];
  return parts.join('-').toLowerCase();
}

function generateTags(rawRow) {
  const tags = [];
  const hospitalized = parseFloat(rawRow.hospitalized) || 0;
  const amputation = parseFloat(rawRow.amputation) || 0;
  const lossOfEye = parseFloat(rawRow.loss_of_eye) || 0;
  
  if (hospitalized > 0) tags.push('hospitalized');
  if (amputation > 0) tags.push('amputation');
  if (lossOfEye > 0) tags.push('loss-of-eye');
  if (rawRow.nature) tags.push(slugify(rawRow.nature));
  
  return tags.slice(0, 5); // Limit tags
}
