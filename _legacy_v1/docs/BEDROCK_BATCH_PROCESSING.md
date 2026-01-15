# Bedrock Batch Processing Strategy

## 🎯 **Overview**

**Goal:** Use Bedrock batch processing (50% cheaper, 24hr delay) for cost optimization while keeping lazy loading instant for user-facing violations.

---

## 📊 **Processing Strategies**

### 1. **Lazy Loading (On-Demand)** - Current
- **When:** User views violation without Bedrock content
- **Speed:** Instant (~2-5 seconds)
- **Cost:** $0.002 per 1K tokens
- **Use Case:** User-facing violations, SEO-critical pages

### 2. **Batch Processing** - New
- **When:** Historical imports, bulk processing
- **Speed:** 24 hours
- **Cost:** $0.001 per 1K tokens (50% discount)
- **Use Case:** Historical data, bulk SEO content, non-urgent violations

---

## 🎚️ **Priority Tiers**

### **Tier 1: High Priority (Instant)**
- Fatalities
- Fines > $100,000
- Recent violations (< 30 days)
- **Processing:** Lazy loading (on-demand)

### **Tier 2: Medium Priority (Batch)**
- Fines $50,000 - $100,000
- Serious violations
- Violations 30-90 days old
- **Processing:** Batch processing

### **Tier 3: Low Priority (Batch or Skip)**
- Fines < $50,000
- Minor violations
- Violations > 90 days old
- **Processing:** Batch processing (configurable)

---

## ⚙️ **Batch Processing Configuration**

### **Configuration Options**

```javascript
{
  // Filter Criteria
  filters: {
    minFineAmount: 0,           // Minimum fine to process
    maxFineAmount: null,        // Maximum fine (null = no limit)
    violationTypes: [],         // Specific types to include (empty = all)
    excludeTypes: [],           // Types to exclude
    dateRange: {
      start: null,              // Start date (null = all)
      end: null                 // End date (null = all)
    },
    agencies: [],               // Specific agencies (empty = all)
    hasFatalities: null         // true/false/null (null = both)
  },
  
  // Processing Limits
  limits: {
    maxViolations: 10000,       // Max violations per batch
    maxSpendUSD: 50,            // Max spend per batch ($)
    maxPercent: 100,            // Max percent of dataset (%)
    priority: 'high'            // 'high' | 'medium' | 'low' | 'all'
  },
  
  // Batch Job Settings
  batch: {
    outputFormat: 'jsonl',      // Bedrock batch output format
    s3InputPrefix: 'bedrock-batches/input/',
    s3OutputPrefix: 'bedrock-batches/output/'
  }
}
```

---

## 🏗️ **Architecture**

### **Components**

1. **Batch Processor Lambda** (`lambda-bedrock-batch-processor.js`)
   - Queries violations from S3 Tables
   - Applies filters and limits
   - Creates Bedrock batch job
   - Tracks job status

2. **Batch Result Handler Lambda** (`lambda-bedrock-batch-result.js`)
   - Triggered by S3 event (when batch completes)
   - Processes batch results
   - Stores in DynamoDB (triggers sync Lambda)

3. **Configuration API** (add to admin API)
   - Manage batch processing settings
   - View batch job status
   - Estimate costs

---

## 📋 **Implementation Plan**

### **Phase 1: Batch Processor Lambda**
- Query violations from S3 Tables
- Apply priority filters
- Create Bedrock batch job
- Store job metadata

### **Phase 2: Batch Result Handler**
- Process completed batch results
- Parse JSONL output
- Store in DynamoDB
- Handle errors

### **Phase 3: Configuration System**
- Admin API endpoints
- Cost estimation
- Job status tracking
- Historical import integration

### **Phase 4: Historical Import Integration**
- Auto-detect high-priority violations
- Queue for batch processing
- Show progress/status

---

## 💰 **Cost Estimation**

### **Example: 100,000 Violations**

**Lazy Loading (On-Demand):**
- Avg tokens per violation: ~300
- Cost per violation: $0.0006
- Total cost: $60

**Batch Processing:**
- Avg tokens per violation: ~300
- Cost per violation: $0.0003 (50% discount)
- Total cost: $30
- **Savings: $30 (50%)**

### **With Priority Filtering**

**Scenario:** Process only violations with fines > $50k (10% of dataset)

- Total violations: 100,000
- High priority: 10,000
- Batch cost: $3
- **Savings: $57 (95%)**

---

## 🚀 **Usage Examples**

### **Historical Import with Batch Processing**

```javascript
// Import violations
await importViolations(violations, {
  path: 'bronze/historical/osha/2024-01-01/',
  batchProcess: {
    enabled: true,
    priority: 'high',
    maxSpendUSD: 50,
    filters: {
      minFineAmount: 50000
    }
  }
});

// Batch processor automatically:
// 1. Queries violations from S3 Tables
// 2. Filters by priority
// 3. Creates Bedrock batch job
// 4. Processes results 24hrs later
```

### **Manual Batch Processing**

```bash
# Trigger batch processing via API
POST /api/admin/bedrock/batch
{
  "filters": {
    "minFineAmount": 50000,
    "agencies": ["osha"]
  },
  "limits": {
    "maxViolations": 5000,
    "maxSpendUSD": 25
  }
}
```

---

## ✅ **Next Steps**

1. Build Batch Processor Lambda
2. Build Batch Result Handler Lambda
3. Add configuration API endpoints
4. Integrate with historical import
5. Add cost estimation
6. Add job status tracking

