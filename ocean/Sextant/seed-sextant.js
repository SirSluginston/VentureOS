import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

const client = new DynamoDBClient({ region: 'us-east-1' });
const TABLE_NAME = 'VentureOS-Sextant';

/* 
    The Sextant Map:
    Maps source-specific CSV headers to our Unified Schema keys.
    GOAL: 100% Coverage. No raw keys left unmapped if they have meaning.
*/

const MAPS = [
    // 1. OSHA - Severe Incident Reports
    {
        PK: 'SOURCE#OSHA',
        SK: 'SCHEMA#severe-incident',
        header_map: {
            "semantic_id": ["ID"],
            "event_date": ["EventDate"],
            "company_name": ["Employer"],
            "street": ["Address1", "Address2"], // Composite or first available
            "city": ["City"],
            "state": ["State"],
            "zip": ["Zip"],
            "location_lat": ["Latitude"],
            "location_lon": ["Longitude"],
            "naics_code": ["Primary NAICS"],
            "description": ["Final Narrative"],
            "violation_type": ["NatureTitle", "EventTitle"], // Title implies readable text
            "violation_code": ["Nature", "Event", "Part of Body", "Source", "Secondary Source"], // Raw codes
            "violation_part": ["Part of Body Title"],
            "violation_source": ["SourceTitle", "Secondary Source Title"],

            // Injury Stats (Normalized to match ODI/ITA)
            "injuries_hospitalized": ["Hospitalized"],
            "injuries_amputation": ["Amputation"],
            "injuries_eye_loss": ["Loss of Eye"],

            "inspection_id": ["Inspection"] // Link to full inspection
        }
    },
    // 2. OSHA - ODI (1996-2001)
    {
        PK: 'SOURCE#OSHA',
        SK: 'SCHEMA#odi-96-01',
        header_map: {
            "data_reliability": ["SURVEYSTATUS"],
            "company_name": ["ESTAB_NAME", "ESTAB_NAME2"],
            "street": ["STREET"],
            "city": ["CITY"],
            "state": ["STATE"],
            "zip": ["ZIP"],
            "event_date": ["Year"],
            "sic_code": ["SIC"],
            "phone": ["PHONE"],

            "avg_annual_employees": ["Q1"],
            "total_hours_worked": ["Q2"],

            "status_normal": ["Q3A"],
            "status_strike": ["Q3B"],
            "status_shutdown": ["Q3C"],
            "status_seasonal": ["Q3D"],
            "status_disaster": ["Q3E"],
            "status_short_schedule": ["Q3F"],
            "status_long_schedule": ["Q3G"],
            "status_other": ["Q3H"],

            "total_injury_deaths": ["C1"],
            "injuries_days_away_restricted": ["C2"],
            "injuries_days_away": ["C3"],
            "total_days_away": ["C4"],
            "total_days_restricted": ["C5"],
            "injuries_no_lost_days": ["C6"],

            "illness_skin": ["C7A"],
            "illness_dust_lung": ["C7B"],
            "illness_respiratory_toxic": ["C7C"],
            "illness_poisoning": ["C7D"],
            "illness_physical_agents": ["C7E"],
            "illness_repeated_trauma": ["C7F"],
            "illness_other": ["C7G"],

            "total_illness_deaths": ["C8"],
            "illnesses_days_away_restricted": ["C9"],
            "illnesses_days_away": ["C10"],
            "illnesses_days_away_total": ["C11"],
            "illnesses_restricted_total": ["C12"],
            "illnesses_no_lost_days": ["C13"]
        }
    },
    // 3. OSHA - ODI (2002-2011)
    {
        PK: 'SOURCE#OSHA',
        SK: 'SCHEMA#odi-02-11',
        header_map: {
            "data_reliability": ["SURVEYSTATUS"],
            "company_name": ["ESTAB_NAME", "ESTAB_NAME2"],
            "street": ["STREET"],
            "city": ["CITY"],
            "state": ["STATE"],
            "zip": ["ZIP"],
            "event_date": ["Year"],
            "sic_code": ["SIC"],
            "naics_code": ["NAICS"],
            "phone": ["PHONE"],

            "avg_annual_employees": ["EMP_Q1"],
            "total_hours_worked": ["HOURS_Q2"],

            "status_normal": ["UNUSUAL_Q3"],
            "status_strike": ["STRIKE_Q3"],
            "status_shutdown": ["SHUT_Q3"],
            "status_seasonal": ["SEASONAL_Q3"],
            "status_disaster": ["DISASTER_Q3"],
            "status_short_schedule": ["SHORT_Q3"],
            "status_long_schedule": ["LONG_Q3"],
            "status_other": ["OREASON_Q3"],
            "status_other_desc": ["OREASON_DESC"],

            "injury_illness_occurred": ["INJILL_Q4"],

            "total_deaths": ["DEATHS_G"],
            "cases_days_away": ["CAWAY_H"],
            "cases_job_transfer": ["CTRANSFER_I"],
            "cases_other": ["COTHER_J"],
            "days_away": ["DAWAY_L"],
            "days_job_transfer": ["DTRANSFER_K"],

            "injuries_total": ["INJ_M1"],
            "illness_skin": ["SKIN_M2"],
            "illness_respiratory": ["RESP_M3"],
            "illness_poisoning": ["POIS_M4"],
            "illness_hearing_loss": ["HEARING_M"],
            "illness_other": ["OTHER_M5"]
        }
    },
    // 4. OSHA - ITA (Injury Tracking Application) - FULLY MAPPED
    {
        PK: 'SOURCE#OSHA',
        SK: 'SCHEMA#ita',
        header_map: {
            "semantic_id": ["id"],
            "company_name": ["company_name", "establishment_name"],
            "ein": ["ein"],
            "street": ["street_address"],
            "city": ["city"],
            "state": ["state"],
            "zip": ["zip_code"],
            "naics_code": ["naics_code"],
            "industry_desc": ["industry_description"],
            "avg_annual_employees": ["annual_average_employees"], // MATCHES ODI
            "total_hours_worked": ["total_hours_worked"], // MATCHES ODI
            "no_injuries_illnesses": ["no_injuries_illnesses"],

            "total_deaths": ["total_deaths"], // MATCHES ODI
            "cases_days_away": ["total_dafw_cases"], // MATCHES ODI (dafw = days away from work)
            "cases_job_transfer": ["total_djtr_cases"], // MATCHES ODI (djtr = days job transfer restriction)
            "cases_other": ["total_other_cases"], // MATCHES ODI
            "days_away": ["total_dafw_days"], // MATCHES ODI
            "days_job_transfer": ["total_djtr_days"], // MATCHES ODI

            "injuries_total": ["total_injuries"], // MATCHES ODI
            "illness_poisoning": ["total_poisonings"], // MATCHES ODI
            "illness_respiratory": ["total_respiratory_conditions"], // MATCHES ODI
            "illness_skin": ["total_skin_disorders"], // MATCHES ODI
            "illness_hearing_loss": ["total_hearing_loss"], // MATCHES ODI
            "illness_other": ["total_other_illnesses"], // MATCHES ODI

            "establishment_id": ["establishment_id"],
            "establishment_type": ["establishment_type"],
            "size": ["size"],
            "event_date": ["year_filing_for", "created_timestamp"],
            "change_reason": ["change_reason"]
        }
    },
];

async function seed() {
    console.log(`🧭 Seeding The Sextant (${TABLE_NAME})...`);

    for (const item of MAPS) {
        try {
            await client.send(new PutItemCommand({
                TableName: TABLE_NAME,
                Item: marshall(item)
            }));
            console.log(`✅ Seeded map for ${item.PK} / ${item.SK}`);
        } catch (error) {
            console.error(`❌ Failed to seed ${item.PK}:`, error);
        }
    }
    console.log("Done.");
}

seed();
