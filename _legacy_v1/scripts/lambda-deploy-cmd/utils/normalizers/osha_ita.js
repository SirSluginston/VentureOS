/**
 * Normalizer for OSHA Injury Tracking Application (ITA) - Annual Summary Data (2016-Present)
 */

export const agency = 'OSHA';
export const table_name = 'osha_ita';

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

export function normalizeItaReport(rawRow, filename) {
    // Basic trim helper
    const t = (val) => val ? String(val).trim() : '';
    const n = (val) => parseInt(val) || 0;
    
    // Attempt to extract year from filename (e.g. FY2024.csv)
    let fileYear = null;
    if (filename) {
        const match = filename.match(/FY(\d{4})/i);
        if (match) fileYear = parseInt(match[1]);
    }
    
    const rowYear = n(rawRow.year_filing_for) || (rawRow.created_timestamp ? new Date(rawRow.created_timestamp).getFullYear() - 1 : null);
    const year = rowYear || fileYear;

    // Skip if no year found
    if (!year) return null;
    
    // Map Fields
    const employerName = t(rawRow.company_name || rawRow.establishment_name);
    const normalized = {
        // Unified Schema Fields
        company_name: employerName, // Map employer to company_name for unified schema
        agency: 'OSHA',
        violation_type: 'Annual Summary',
        fine_amount: 0, // ITA reports don't have fines
        
        // Location (normalized)
        state: normalizeState(t(rawRow.state)),
        city: normalizeCity(t(rawRow.city)),
        
        // Domain-specific fields (will go to violation_details)
        employer: employerName,
        street: t(rawRow.street_address),
        zip: t(rawRow.zip_code),
        year: year,
        
        // Employee Stats
        annual_average_employees: n(rawRow.annual_average_employees),
        total_hours_worked: n(rawRow.total_hours_worked),
        
        // Industry Codes
        naics_code: t(rawRow.naics_code),
        industry_description: t(rawRow.industry_description),
        
        // Metrics
        total_deaths: n(rawRow.total_deaths),
        total_days_away_cases: n(rawRow.total_dafw_cases),
        total_days_away_count: n(rawRow.total_dafw_days),
        total_job_transfer_cases: n(rawRow.total_djtr_cases),
        total_days_transfer_count: n(rawRow.total_djtr_days),
        total_other_cases: n(rawRow.total_other_cases),
        
        // Additional Health Details (ITA specific)
        total_injuries: n(rawRow.total_injuries),
        total_skin_disorders: n(rawRow.total_skin_disorders),
        total_respiratory_conditions: n(rawRow.total_respiratory_conditions),
        total_poisonings: n(rawRow.total_poisonings),
        total_hearing_loss: n(rawRow.total_hearing_loss),
        total_other_illnesses: n(rawRow.total_other_illnesses),
        
        // Raw Data Backup
        raw_data: JSON.stringify(rawRow)
    };

    // Calculate Rates (TCR, DART)
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

    // Add Standard Violation Fields
    const safeEmployer = normalized.employer.replace(/[^a-zA-Z0-9]/g, '').slice(0, 10);
    normalized.violation_id = `ITA-${normalized.year}-${safeEmployer}-${Math.random().toString(36).substring(7)}`;
    
    normalized.raw_title = `Annual Safety Summary (${normalized.year})`;
    
    // Construct rich description
    const parts = [];
    if (normalized.total_deaths > 0) parts.push(`${normalized.total_deaths} Deaths`);
    if (normalized.total_days_away_cases > 0) parts.push(`${normalized.total_days_away_cases} Cases w/ Days Away`);
    if (normalized.total_job_transfer_cases > 0) parts.push(`${normalized.total_job_transfer_cases} Job Transfers`);
    
    // Health breakdown
    const health = [];
    if (normalized.total_injuries > 0) health.push(`${normalized.total_injuries} Injuries`);
    if (normalized.total_respiratory_conditions > 0) health.push(`${normalized.total_respiratory_conditions} Respiratory`);
    if (normalized.total_hearing_loss > 0) health.push(`${normalized.total_hearing_loss} Hearing Loss`);
    
    let desc = `Annual Summary: ${parts.length > 0 ? parts.join(', ') : 'No lost time incidents'}.`;
    if (health.length > 0) desc += ` Breakdown: ${health.join(', ')}.`;
    
    desc += ` TCR: ${normalized.calculated_tcr}, DART: ${normalized.calculated_dart}. Employees: ${normalized.annual_average_employees}.`;
    
    normalized.raw_description = desc;

    normalized.event_date = normalized.year ? `${normalized.year}-12-31` : null;
    
    // Add slugs (use company_name for slug generation)
    normalized.company_slug = normalized.company_name
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
        
    // Optional unified schema fields
    normalized.source_url = null;
    normalized.tags = []; // Can add tags based on metrics if needed
    
    return normalized;
}

export const ROUTES = {
    'osha-ita': normalizeItaReport
};

