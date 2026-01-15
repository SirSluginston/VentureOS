/**
 * Create Test Violations Script
 * 
 * Uploads test violations to Bronze folder to trigger the parquet writer Lambda
 * Then tests the consolidator with days=0
 */

import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';

const s3Client = new S3Client({ region: 'us-east-1' });
const lambdaClient = new LambdaClient({ region: 'us-east-1' });

const BUCKET = 'sirsluginston-ventureos-data';

// Test violations in raw OSHA Severe Injury Report format (what the normalizer expects)
// Based on normalizeSevereInjuryReport function
const testViolations = [
  {
    id: "TEST-001",
    eventdate: "2024-01-15",
    employer: "Test Construction Co",
    city: "Houston",
    state: "TX",
    final_narrative: "Worker lost finger in unguarded press. Employee was operating machinery without proper safety guards.",
    nature: "Amputation",
    part_of_body: "Finger",
    event: "Contact with object",
    source: "Machinery",
    hospitalized: "1",
    amputation: "1",
    loss_of_eye: "0",
    upa: "TEST-UPA-001",
    inspection: "TEST-INSP-001",
    zip: "77001",
    primary_naics: "236220"
  },
  {
    id: "TEST-002",
    eventdate: "2024-02-20",
    employer: "Demo Manufacturing",
    city: "Austin",
    state: "TX",
    final_narrative: "Fall from height due to missing guardrails. Worker fell 10 feet from scaffold platform.",
    nature: "Fracture",
    part_of_body: "Leg",
    event: "Fall",
    source: "Scaffold",
    hospitalized: "1",
    amputation: "0",
    loss_of_eye: "0",
    upa: "TEST-UPA-002",
    inspection: "TEST-INSP-002",
    zip: "78701",
    primary_naics: "331110"
  },
  {
    id: "TEST-003",
    eventdate: "2024-03-10",
    employer: "Sample Industries",
    city: "Dallas",
    state: "TX",
    final_narrative: "Cut from unguarded blade. Employee suffered deep laceration requiring stitches.",
    nature: "Laceration",
    part_of_body: "Hand",
    event: "Contact with object",
    source: "Blade",
    hospitalized: "0",
    amputation: "0",
    loss_of_eye: "0",
    upa: "TEST-UPA-003",
    inspection: "TEST-INSP-003",
    zip: "75201",
    primary_naics: "332710"
  }
];

async function createTestViolations() {
  console.log('📝 Creating test violations...');
  
  // Upload to bronze/daily/osha/severe_injury/{today}/
  const today = new Date().toISOString().split('T')[0];
  const key = `bronze/daily/osha/severe_injury/${today}/test-violations.json`;
  
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    Body: JSON.stringify(testViolations),
    ContentType: 'application/json'
  }));
  
  console.log(`✅ Uploaded test violations to: s3://${BUCKET}/${key}`);
  console.log('⏳ Waiting 10 seconds for Lambda to process...');
  
  // Wait for Lambda to process
  await new Promise(resolve => setTimeout(resolve, 10000));
  
  console.log('✅ Test violations should now be in buffer folder');
}

async function testConsolidator() {
  console.log('\n🚜 Testing consolidator with days=0...');
  
  const response = await lambdaClient.send(new InvokeCommand({
    FunctionName: 'ventureos-consolidator',
    Payload: JSON.stringify({
      days: 0,
      dryRun: false
    })
  }));
  
  const result = JSON.parse(new TextDecoder().decode(response.Payload));
  console.log('📊 Consolidator Result:', JSON.stringify(result, null, 2));
}

async function main() {
  try {
    await createTestViolations();
    await testConsolidator();
    console.log('\n✅ Test complete!');
  } catch (error) {
    console.error('❌ Error:', error);
    process.exit(1);
  }
}

main();

