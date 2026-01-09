/**
 * VentureOS Query API (v2)
 * 
 * Serves unified data from S3 Parquet Data Lake (Archive + Buffer).
 * Handles API Gateway events and executes parameterized DuckDB queries.
 */

import { DuckDBInstance } from '@duckdb/node-api';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';

const lambdaClient = new LambdaClient({ region: process.env.AWS_REGION || 'us-east-1' });
const BEDROCK_GENERATOR_FUNCTION = process.env.BEDROCK_GENERATOR_FUNCTION || 'ventureos-bedrock-generator';

const BUCKET = 'sirsluginston-ventureos-data';
const ARCHIVE_PATH = `s3://${BUCKET}/silver/violations/archive/**/*.parquet`;
const BUFFER_PATH = `s3://${BUCKET}/silver/violations/buffer/**/*.parquet`;

async function initDuckDB() {
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  await con.run("SET temp_directory='/tmp/duckdb_temp'");
  await con.run("SET home_directory='/tmp'");
  await con.run("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;");
  
  if (process.env.AWS_LAMBDA_FUNCTION_NAME) {
    await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  } else {
    // Local fallback
    await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  }

  // Enable Hive Partitioning to auto-detect violation_year / ingest_date
  // await con.run("SET hive_partitioning=true;"); // Deprecated/Not supported globally

  // Create Unified View (Buffer + Archive)
  // Strategy: Try combined, then individual, then empty fallback
  // This handles cases where one folder is empty (DuckDB throws on empty glob matches)
  
  let viewCreated = false;
  
  // 1. Try Combined
  try {
      await con.run(`
        CREATE VIEW violations AS 
        SELECT * FROM read_parquet(['${ARCHIVE_PATH}', '${BUFFER_PATH}'], union_by_name=true, hive_partitioning=true)
      `);
      viewCreated = true;
  } catch (e) { 
      console.warn("⚠️ Combined view failed (likely empty path), retrying individual...", e.message); 
  }

  // 2. Try Archive Only (if combined failed)
  if (!viewCreated) {
    try {
        await con.run(`
          CREATE VIEW violations AS 
          SELECT * FROM read_parquet('${ARCHIVE_PATH}', union_by_name=true, hive_partitioning=true)
        `);
        viewCreated = true;
    } catch (e) { 
        console.warn("⚠️ Archive view failed...", e.message); 
    }
  }

  // 3. Try Buffer Only (if archive failed)
  if (!viewCreated) {
    try {
        await con.run(`
          CREATE VIEW violations AS 
          SELECT * FROM read_parquet('${BUFFER_PATH}', union_by_name=true, hive_partitioning=true)
        `);
        viewCreated = true;
    } catch (e) { 
        console.warn("⚠️ Buffer view failed...", e.message); 
    }
  }

  // 4. Fallback: Empty View
  if (!viewCreated) {
      console.warn("⚠️ No data found in S3! Creating empty view.");
      await con.run(`
          CREATE VIEW violations AS 
          SELECT 
            NULL::VARCHAR as agency, 
            NULL::VARCHAR as city, 
            NULL::VARCHAR as state, 
            NULL::VARCHAR as company_name, 
            NULL::VARCHAR as company_slug,
            NULL::DATE as event_date, 
            NULL::DOUBLE as fine_amount, 
            NULL::VARCHAR as violation_type, 
            NULL::VARCHAR as bedrock_title,
            NULL::VARCHAR as bedrock_description
          WHERE 1=0
      `);
  }

  return { db, con };
}

export async function handler(event) {
  // Normalize Event (Support API Gateway v1 & Function URL v2)
  // For proxy routes like /api/{proxy+}, reconstruct full path from pathParameters
  let path = event.path || event.rawPath || '';
  if (!path && event.pathParameters?.proxy) {
    // Reconstruct path from proxy parameter
    path = `/api/${event.pathParameters.proxy}`;
  }
  const method = event.httpMethod || (event.requestContext?.http?.method) || 'GET';
  const queryParams = event.queryStringParameters || {};

  console.log('📥 API Event:', JSON.stringify({ path, params: queryParams, method, pathParameters: event.pathParameters }));
  
  // 0. Handle CORS Preflight (OPTIONS)
  if (method === 'OPTIONS') {
    console.log('✅ OPTIONS request - returning CORS headers');
    return apiResponse(200, {});
  }
  
  let dbInstance, connection;

  try {
    const { db, con } = await initDuckDB();
    dbInstance = db;
    connection = con;

    // Helper for parameterized queries
    const query = async (sql, ...params) => {
        if (params.length > 0) {
            const stmt = await con.prepare(sql);
            stmt.bind(params);
            const result = await stmt.run();
            return await result.getRowObjectsJson();
        } else {
            const result = await con.run(sql);
            return await result.getRowObjectsJson();
        }
    };

    // 1. Raw Query Mode (Internal/Admin Testing)
    if (event.query) {
       console.log('⚠️ Executing Raw Query (Admin Mode)');
       const params = (event.params && Array.isArray(event.params)) ? event.params : [];
       const results = await query(event.query, ...params);
       return { statusCode: 200, body: JSON.stringify(results) };
    }

    // 2. API Gateway Routing
    const limit = Math.min(parseInt(queryParams.limit || '50'), 100); // Max 100 rows
    const offset = parseInt(queryParams.page || '0') * limit;

    // Route: /api/city/{name} (Standard V1 Route)
    if (path.match(/\/api\/city\/.+/)) {
      const city = decodeURIComponent(path.split('/').pop());
      const state = queryParams.state; // Optional state filter
      
      console.log(`🏙️ Fetching City: ${city} (State: ${state || 'Any'})`);
      
      let sql = `
        SELECT agency, city, state, company_name, event_date, fine_amount, violation_type, bedrock_title, bedrock_description
        FROM violations 
        WHERE city = ? 
      `;
      const args = [city];

      if (state) {
        sql += ` AND state = ?`;
        args.push(state);
      }

      sql += ` ORDER BY event_date DESC LIMIT ? OFFSET ?`;
      args.push(limit, offset);

      const results = await query(sql, ...args);
      
      const response = {
        city: city,
        state: state || 'Unknown',
        violations: results,
        companies: [], // Aggregation needed for full parity
        stats: {
           totalViolations: results.length, // Only counting this page? No, meta.count
           totalFines: results.reduce((sum, v) => sum + (v.fine_amount || 0), 0),
           averageFine: 0
        },
        meta: {
          totalViolations: results.length, // Should be total count from DB, not page
          displayedViolations: results.length
        }
      };
      
      return apiResponse(200, response);
    }

    // Route: /api/company/{slug}
    if (path.match(/\/api\/company\/.+/)) {
      const slug = path.split('/').pop();
      console.log(`🏢 Fetching Company: ${slug}`);
      
      const sql = `
        SELECT * 
        FROM violations 
        WHERE company_slug = ? 
        ORDER BY event_date DESC LIMIT ? OFFSET ?
      `;
      const results = await query(sql, slug, limit, offset);
      
      const response = {
        company: slug,
        violations: results,
        stats: { totalViolations: results.length, totalFines: 0, averageFine: 0 },
        meta: { totalViolations: results.length }
      };
      return apiResponse(200, response);
    }

    // Route: /api/state/{state}
    if (path.match(/\/api\/state\/.+/)) {
      const state = decodeURIComponent(path.split('/').pop()).toUpperCase();
      console.log(`🗺️ Fetching State: ${state}`);
      
      const sql = `
        SELECT agency, city, state, company_name, event_date, fine_amount, violation_type, bedrock_title, bedrock_description
        FROM violations 
        WHERE state = ? 
        ORDER BY event_date DESC LIMIT ? OFFSET ?
      `;
      const results = await query(sql, state, limit, offset);
      
      const response = {
        state: state,
        violations: results,
        cities: [], // Aggregation needed
        stats: { 
          totalViolations: results.length, // approximation based on page
          totalFines: results.reduce((sum, v) => sum + (v.fine_amount || 0), 0), 
          averageFine: 0 
        },
        meta: { totalViolations: results.length }
      };
      return apiResponse(200, response);
    }

    // Route: /api/violation-types/{type}
    if (path.match(/\/api\/violation-types\/.+/)) {
      const typeSlug = path.split('/').pop();
      const type = decodeURIComponent(typeSlug).replace(/-/g, ' ');
      console.log(`⚠️ Fetching Violation Type: ${type}`);

      const sql = `
        SELECT * 
        FROM violations 
        WHERE violation_type ILIKE ? 
        ORDER BY event_date DESC LIMIT ? OFFSET ?
      `;
      const results = await query(sql, type, limit, offset);
      
      const response = {
        violationType: type,
        violations: results,
        stats: { 
          totalViolations: results.length, 
          totalFines: results.reduce((sum, v) => sum + (v.fine_amount || 0), 0),
          averageFine: 0
        },
        meta: { totalViolations: results.length }
      };
      return apiResponse(200, response);
    }

    // Route: /api/national (Stats)
    if (path.includes('/api/national')) {
      console.log('🇺🇸 Fetching National Stats');
      // Aggregation query
      const stats = await query(`
        SELECT 
          COUNT(*) as totalViolations,
          SUM(fine_amount) as totalFines,
          COUNT(DISTINCT state) as totalStates,
          COUNT(DISTINCT city) as totalCities
        FROM violations
      `);
      
      const states = await query(`
        SELECT 
          state, 
          COUNT(*) as violationCount, 
          COUNT(DISTINCT city) as cityCount 
        FROM violations 
        GROUP BY state 
        ORDER BY state ASC
      `);

      const response = {
        stats: {
          totalStates: Number(stats[0].totalStates),
          totalCities: Number(stats[0].totalCities),
          totalViolations: Number(stats[0].totalViolations),
          totalFines: Number(stats[0].totalFines)
        },
        states: states.map(s => ({
          state: s.state,
          cityCount: Number(s.cityCount),
          violationCount: Number(s.violationCount)
        }))
      };
      return apiResponse(200, response);
    }

    // Route: /api/cities (Directory)
    if (path.includes('/api/cities')) {
      console.log('🏙️ Fetching All Cities Directory');
      const sql = `
        SELECT 
          city as name, 
          city as slug,
          COUNT(*) as violationCount 
        FROM violations 
        GROUP BY city 
        ORDER BY violationCount DESC 
        LIMIT ?
      `;
      
      const results = await query(sql, limit || 100);
      
      return apiResponse(200, { 
        cities: results.map(r => ({ ...r, violationCount: Number(r.violationCount) })), 
        total: results.length 
      });
    }

    // Route: /api/violation-types (Directory)
    if (path.includes('/api/violation-types')) {
      console.log('⚠️ Fetching Violation Types Directory');
      const results = await query(`
        SELECT 
          violation_type as type, 
          violation_type as slug, 
          COUNT(*) as violationCount 
        FROM violations 
        GROUP BY violation_type 
        ORDER BY violationCount DESC
      `);
      
      return apiResponse(200, { 
        violationTypes: results.map(r => ({ ...r, violationCount: Number(r.violationCount) })), 
        total: results.length 
      });
    }

    // Route: /api/recent (if used by V1)
    if (path.includes('/api/recent')) {
       console.log('🕒 Fetching Recent Violations');
       const sql = `SELECT * FROM violations ORDER BY event_date DESC LIMIT ?`;
       const results = await query(sql, limit);
       return apiResponse(200, { data: results });
    }

    // Route: /api/search/company (Fuzzy search)
    if (path.includes('/api/search/company')) {
      const searchQuery = queryParams.q || '';
      const searchLimit = parseInt(queryParams.limit || '3');
      console.log('🔍 Searching Companies:', searchQuery);
      
      const sql = `
        SELECT 
          company_name as name,
          company_slug as slug,
          COUNT(*) as violationCount
        FROM violations 
        WHERE LOWER(company_name) LIKE LOWER(?)
        GROUP BY company_name, company_slug
        ORDER BY violationCount DESC
        LIMIT ?
      `;
      
      const results = await query(sql, `%${searchQuery}%`, searchLimit);
      return apiResponse(200, { companies: results.map(r => ({ ...r, violationCount: Number(r.violationCount) })) });
    }

    // Route: /api/search/city (Fuzzy search)
    if (path.includes('/api/search/city')) {
      const searchQuery = queryParams.q || '';
      const searchLimit = parseInt(queryParams.limit || '3');
      console.log('🔍 Searching Cities:', searchQuery);
      
      const sql = `
        SELECT 
          city as name,
          city as slug,
          COUNT(*) as violationCount
        FROM violations 
        WHERE LOWER(city) LIKE LOWER(?)
        GROUP BY city
        ORDER BY violationCount DESC
        LIMIT ?
      `;
      
      const results = await query(sql, `%${searchQuery}%`, searchLimit);
      return apiResponse(200, { cities: results.map(r => ({ ...r, violationCount: Number(r.violationCount) })) });
    }

    // Route: /api/bedrock/generate (Trigger Bedrock content generation)
    if (path.includes('/api/bedrock/generate') && event.httpMethod === 'POST') {
      return await handleBedrockGenerate(event);
    }

    return apiResponse(404, { error: `Route not found: ${path}` });

  } catch (error) {
    console.error('❌ API Error:', error);
    return apiResponse(500, { error: error.message });
  } finally {
    if (connection) {
        try { await connection.close(); } catch (e) { console.warn('Error closing connection:', e); }
    }
    if (dbInstance) {
        // DuckDBInstance uses closeSync (void) or maybe just close() if updated?
        // Documentation says closeSync.
        try { dbInstance.closeSync(); } catch (e) { console.warn('Error closing instance:', e); }
    }
  }
}

/**
 * Handle Bedrock generation request - invokes Bedrock Generator Lambda asynchronously
 */
async function handleBedrockGenerate(event) {
  try {
    let body;
    if (typeof event.body === 'string') {
      body = JSON.parse(event.body);
    } else {
      body = event.body || {};
    }
    
    const violationId = body.violation_id;
    
    if (!violationId) {
      return apiResponse(400, { error: 'violation_id is required' });
    }
    
    // Invoke Bedrock Generator Lambda asynchronously (fire-and-forget)
    await lambdaClient.send(new InvokeCommand({
      FunctionName: BEDROCK_GENERATOR_FUNCTION,
      InvocationType: 'Event', // Async invocation
      Payload: JSON.stringify({
        body: JSON.stringify({ violation_id: violationId })
      })
    }));
    
    return apiResponse(202, { 
      message: 'Bedrock content generation started',
      violation_id: violationId
    });
    
  } catch (error) {
    console.error('Error invoking Bedrock Generator:', error);
    return apiResponse(500, { error: error.message });
  }
}

function apiResponse(code, body) {
  return {
    statusCode: code,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*', // CORS
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    },
    body: JSON.stringify(body)
  };
}
