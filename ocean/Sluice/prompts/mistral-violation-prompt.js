// Mistral Large (24.02) Prompt for Individual Violation Processing
// Model: mistral.mistral-large-2402-v1:0
// Settings: Temperature 0.2, Top P 0.9, Max Tokens 1500

/**
 * Get the system message for Mistral Large
 */
export function getSystemMessage() {
  return `You are a workplace safety data analyst. Convert OSHA violation data into structured, neutral, factual descriptions.

CRITICAL RULES:
- Use ONLY information provided in the violation data
- Do NOT invent, infer, or assume details
- Write in neutral, professional, journalistic tone
- No sarcasm, humor, or editorial commentary
- Return ONLY valid JSON, no markdown or code blocks

OUTPUT FORMAT:
Return JSON with these fields:
- title: Brief descriptive title (max 80 chars)
- explanation: What happened (2-4 sentences, neutral)
- riskAssessment: Safety risk assessment (2-3 sentences)
- compliance: OSHA compliance context (1-2 sentences, optional)
- keyPoints: Array of 3-5 key points
- industryContext: Industry context from NAICS (1 sentence, optional)`;
}

/**
 * Build user message with violation data
 * @param {Object} violationData - Normalized violation data from DynamoDB
 * @returns {string} User message content
 */
export function getUserMessage(violationData) {
  const {
    id,
    oshaId,
    eventDate,
    violationType,
    location,
    establishment,
    injury,
  } = violationData;

  // Build minimal violation data JSON for prompt
  const violationJson = JSON.stringify({
    id: id || oshaId || '',
    oshaId: oshaId || id || '',
    eventDate: eventDate || '',
    violationType: violationType || '',
    location: {
      city: location?.city || '',
      state: location?.state || '',
    },
    establishment: {
      name: establishment?.name || '',
      naics: establishment?.naics || '',
    },
    injury: {
      hospitalized: injury?.hospitalized || 0,
      amputation: injury?.amputation || 0,
      lossOfEye: injury?.lossOfEye || 0,
      narrative: injury?.narrative || '',
      nature: injury?.nature || '',
      bodyPart: injury?.bodyPart || '',
      event: injury?.event || '',
      source: injury?.source || '',
    },
  }, null, 2);

  return `Convert the following OSHA violation data into a structured description. Use ONLY the information provided. Do not invent or infer details.

VIOLATION DATA:
${violationJson}

Return a JSON object with the following structure:
{
  "title": "Brief descriptive title (max 80 chars)",
  "explanation": "What happened (2-4 sentences, neutral)",
  "riskAssessment": "Safety risk assessment (2-3 sentences)",
  "compliance": "OSHA compliance context (1-2 sentences, optional)",
  "keyPoints": ["Point 1", "Point 2", "Point 3"],
  "industryContext": "Industry context from NAICS (1 sentence, optional)"
}`;
}

/**
 * Build complete messages array for Mistral Large API
 * @param {Object} violationData - Normalized violation data
 * @returns {Array} Messages array for Bedrock API
 */
export function buildMistralMessages(violationData) {
  return [
    {
      role: 'system',
      content: getSystemMessage(),
    },
    {
      role: 'user',
      content: getUserMessage(violationData),
    },
  ];
}

/**
 * Expected output schema for validation
 */
export const OUTPUT_SCHEMA = {
  title: 'string (max 80 characters)',
  explanation: 'string (2-4 sentences)',
  riskAssessment: 'string (2-3 sentences)',
  compliance: 'string (1-2 sentences, optional)',
  keyPoints: 'array of strings (3-5 items)',
  industryContext: 'string (1 sentence, optional)',
};

