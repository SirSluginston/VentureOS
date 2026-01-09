import { consolidateBuffer } from './utils/consolidator.js';

export async function handler(event) {
  console.log('🚜 Consolidator Lambda Started');
  
  // Default to 30 days, allow override via event
  // e.g. { "days": 0 } to force consolidate everything immediately
  const days = event.days !== undefined ? event.days : 30;
  const dryRun = event.dryRun || false;
  
  try {
    const result = await consolidateBuffer(days, dryRun);
    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Consolidation Complete', stats: result })
    };
  } catch (error) {
    console.error(error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
}



