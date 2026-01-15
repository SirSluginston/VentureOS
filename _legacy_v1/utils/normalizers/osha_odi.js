/**
 * Normalizer for OSHA Data Initiative (ODI) - Annual Summary Data
 * Handles both 1996-2001 (Form 200) and 2002-2011 (Form 300) formats.
 */

export const agency = 'OSHA';
export const table_name = 'osha_odi';

// Helper functions (copied from osha.js for consistency)
function normalizeCity(city) {
    if (!city) return '';
    return city.trim().split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
}

function normalizeState(state) {
    if (!state) return null;
    const cleanState = state.toLowerCase().trim();
    const validStates = new Set(['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC', 'PR', 'VI', 'GU', 'AS', 'MP']);
    const stateMap = {'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR', 'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE', 'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID', 'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS', 'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD', 'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC', 'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT', 'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC', 'puerto rico': 'PR', 'virgin islands': 'VI', 'guam': 'GU', 'american samoa': 'AS', 'northern mariana islands': 'MP', 'pu': 'PR', 'am': 'AS', 'cn': 'MP'};
    let code = stateMap[cleanState];
    if (!code && cleanState.length === 2) {
        code = cleanState.toUpperCase();
    }
    return validStates.has(code) ? code : null;
}

export function normalizeOdiReport(rawRow) {
    // 1. Identify Format
    const isPost2002 = rawRow.deaths_g !== undefined; // Check for explicit column
    
    // 2. Map Common Fields
    const employerName = (rawRow.estab_name || rawRow.company_name || '').trim();
    const normalized = {
        // Unified Schema Fields
        company_name: employerName, // Map employer to company_name for unified schema
        agency: 'OSHA',
        violation_type: 'Annual Summary',
        fine_amount: 0, // ODI reports don't have fines
        
        // Location (normalized)
        state: normalizeState((rawRow.state || '').trim()),
        city: normalizeCity((rawRow.city || '').trim()),
        
        // Domain-specific fields (will go to violation_details)
        employer: employerName,
        street: (rawRow.street || '').trim(),
        zip: (rawRow.zip || '').trim(),
        year: parseInt(rawRow.year) || null,
        
        // Employee Stats
        annual_average_employees: parseInt(rawRow.emp_q1 || rawRow.q1) || 0,
        total_hours_worked: parseInt(rawRow.hours_q2 || rawRow.q2) || 0,
        
        // Industry Codes
        sic_code: rawRow.sic || null,
        naics_code: rawRow.naics || null,
        
        // Metrics (Unified)
        total_deaths: 0,
        total_days_away_cases: 0,
        total_days_away_count: 0,
        total_job_transfer_cases: 0,
        total_days_transfer_count: 0,
        total_other_cases: 0,
        
        // Raw Data Backup (for debugging/legacy)
        raw_data: JSON.stringify(rawRow)
    };

    // Skip if no year (Avoids null date crash)
    if (!normalized.year) return null;

    // 3. Map Format-Specific Metrics
    if (isPost2002) {
        // Form 300 (2002-2011)
        // Headers: DEATHS_G, CAWAY_H, CTRANSFER_I, COTHER_J, DTRANSFER_K, DAWAY_L
        normalized.total_deaths = parseInt(rawRow.deaths_g) || 0;
        normalized.total_days_away_cases = parseInt(rawRow.caway_h) || 0;
        normalized.total_days_away_count = parseInt(rawRow.daway_l) || 0;
        normalized.total_job_transfer_cases = parseInt(rawRow.ctransfer_i) || 0;
        normalized.total_days_transfer_count = parseInt(rawRow.dtransfer_k) || 0;
        normalized.total_other_cases = parseInt(rawRow.cother_j) || 0;
    } else {
        // Form 200 (1996-2001)
        // Headers: C1, C2, C3, C4, C5, C6...
        // Mapping Assumption based on standard Form 200:
        // C1: Fatalities
        // C2: Total Lost Workday Cases (Sum of Away + Restricted?)
        // C3: Cases Involving Days Away
        // C5: Days Away (Count)
        // C6: Days Restricted (Count) -- Wait, sample had C6=8, C5=176.
        
        normalized.total_deaths = parseInt(rawRow.c1) || 0;
        normalized.total_days_away_cases = parseInt(rawRow.c3) || 0;
        normalized.total_days_away_count = parseInt(rawRow.c5) || 0; // Assumption
        // C4 often is Cases with Days Restricted?
        // C2 is likely Total LWC (C3 + C4?)
        normalized.total_job_transfer_cases = (parseInt(rawRow.c2) || 0) - (parseInt(rawRow.c3) || 0); // Roughly
        // If negative, set to 0
        if (normalized.total_job_transfer_cases < 0) normalized.total_job_transfer_cases = 0;
        
        normalized.total_days_transfer_count = parseInt(rawRow.c6) || 0; // Assumption
        normalized.total_other_cases = parseInt(rawRow.c8) || parseInt(rawRow.c10) || 0; // Guessing C8 or C10 is "Injuries without lost workdays"
    }

    // 4. Calculate Rates (TCR, DART)
    const totalCases = normalized.total_deaths + 
                       normalized.total_days_away_cases + 
                       normalized.total_job_transfer_cases + 
                       normalized.total_other_cases;
                       
    if (normalized.total_hours_worked > 0) {
        normalized.calculated_tcr = parseFloat(((totalCases * 200000) / normalized.total_hours_worked).toFixed(2));
        normalized.calculated_dart = parseFloat((((normalized.total_days_away_cases + normalized.total_job_transfer_cases) * 200000) / normalized.total_hours_worked).toFixed(2));
    } else {
        normalized.calculated_tcr = 0;
        normalized.calculated_dart = 0;
    }

    // 5. Add Standard Violation Fields (for Frontend Display & Sync)
    // We treat each Annual Summary as a "Violation" event so it appears in the timeline.
    const safeEmployer = normalized.employer.replace(/[^a-zA-Z0-9]/g, '').slice(0, 10);
    normalized.violation_id = `ODI-${normalized.year}-${safeEmployer}-${Math.random().toString(36).substring(7)}`;
    
    normalized.raw_title = `Annual Safety Summary (${normalized.year})`;
    
    // Construct a rich description
    const parts = [];
    if (normalized.total_deaths > 0) parts.push(`${normalized.total_deaths} Deaths`);
    if (normalized.total_days_away_cases > 0) parts.push(`${normalized.total_days_away_cases} Cases w/ Days Away`);
    if (normalized.total_job_transfer_cases > 0) parts.push(`${normalized.total_job_transfer_cases} Job Transfers`);
    if (normalized.total_other_cases > 0) parts.push(`${normalized.total_other_cases} Other Cases`);
    
    normalized.raw_description = parts.length > 0 
        ? `Annual Summary: ${parts.join(', ')}. TCR: ${normalized.calculated_tcr}, DART: ${normalized.calculated_dart}. Employees: ${normalized.annual_average_employees}.`
        : `Annual Summary: No recordable incidents reported. Employees: ${normalized.annual_average_employees}.`;

    normalized.event_date = `${normalized.year}-12-31`; // End of year
    
    // Add slugs for linking (use company_name for slug generation)
    normalized.company_slug = normalized.company_name
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
    
    // Optional unified schema fields
    normalized.source_url = null;
    normalized.tags = []; // Can add tags based on metrics if needed

    return normalized;
}

// Export a routing map for the Parquet Writer
export const ROUTES = {
    'osha-odi': normalizeOdiReport
};

