# Bedrock Prompts for OSHA Trail

## Individual Violation Processing

### Mistral Large (24.02) - `mistral-violation-prompt.js`

**Purpose**: Convert raw OSHA violation data into structured, neutral, factual descriptions.

**Model**: `mistral.mistral-large-2402-v1:0`
**Settings**:
- Temperature: 0.2
- Top P: 0.9
- Max Tokens: 1500

**Usage**:
```javascript
import { buildMistralMessages } from './prompts/mistral-violation-prompt.js';
import { postProcessBedrockResponse } from './utils/bedrockPostProcessor.js';

// Build messages for Bedrock
const messages = buildMistralMessages(violationData);

// Call Bedrock API (pseudo-code)
const bedrockResponse = await bedrockClient.invokeModel({
  modelId: 'mistral.mistral-large-2402-v1:0',
  messages,
  temperature: 0.2,
  topP: 0.9,
  maxTokens: 1500,
});

// Post-process to add severity and oshaSource
const processedContent = postProcessBedrockResponse(bedrockResponse, violationData);
```

**Bedrock Output** (what Bedrock generates):
```json
{
  "title": "Brief descriptive title",
  "explanation": "2-4 sentence explanation",
  "riskAssessment": "2-3 sentence risk assessment",
  "compliance": "OSHA compliance context (optional)",
  "keyPoints": ["Point 1", "Point 2", "Point 3"],
  "industryContext": "Industry context (optional)"
}
```

**Final Output** (after post-processing):
```json
{
  "title": "...",
  "explanation": "...",
  "riskAssessment": "...",
  "compliance": "...",
  "keyPoints": [...],
  "industryContext": "...",
  "severityLabel": "High",
  "severityScore": 7,
  "oshaSource": {
    "id": "2015010015",
    "url": "https://www.osha.gov/establishments/2015010015",
    "inspection": "1018519",
    "upa": "931176",
    "attribution": "Source: U.S. Department of Labor, OSHA.gov"
  }
}
```

## Key Features

- ✅ Neutral, factual tone (no sarcasm)
- ✅ No fabrication (only uses provided data)
- ✅ Structured JSON output
- ✅ Handles missing fields gracefully
- ✅ Industry context from NAICS codes
- ✅ Programmatic severity calculation
- ✅ Automatic oshaSource attribution

## Files

- `mistral-violation-prompt.js` - Prompt builder functions
- `../utils/severityCalculator.js` - Severity calculation logic
- `../utils/bedrockPostProcessor.js` - Post-processing and merging


