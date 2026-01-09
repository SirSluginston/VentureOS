# Admin UI Design - Batch Processing & Data Upload

## 🎯 **Overview**

Admin UI for managing Bedrock batch processing and uploading historical data.

**Key Features:**
- Batch processing configuration and monitoring
- Direct S3 upload (presigned URLs) - no Lambda costs
- Cost estimation
- Job status tracking

---

## 📤 **Upload Strategy: Presigned URLs**

**Why Presigned URLs?**
- ✅ **No Lambda costs** - Direct browser → S3 upload
- ✅ **No timeout issues** - Lambda has 15min max, large files can take longer
- ✅ **Faster** - Direct upload, no proxy through API Gateway
- ✅ **Same cost** - Data transfer cost is identical either way

**How It Works:**
1. Admin clicks "Upload Data"
2. Frontend requests presigned URL from API
3. API returns presigned URL (valid for 1 hour)
4. Frontend uploads directly to S3 using presigned URL
5. S3 event triggers Parquet Writer Lambda
6. Pipeline processes automatically

---

## 🖥️ **Admin UI Components**

### **1. Batch Processing Section**

**Features:**
- Configure batch job filters
- Set limits (count, percent, spend)
- Estimate cost before running
- View job status
- Cancel jobs

**UI Elements:**
- Filter form (fine amount, dates, agencies, etc.)
- Limit controls (sliders/inputs)
- Cost estimator (real-time)
- Job list/table
- Job status cards

### **2. Data Upload Section**

**Features:**
- Upload CSV/JSON files
- Select agency
- Select date
- Auto-generate S3 path
- Upload progress bar
- Upload history

**UI Elements:**
- File upload (drag & drop)
- Agency selector
- Date picker
- Upload button
- Progress indicator
- Upload log

### **3. Dashboard Section**

**Features:**
- Recent batch jobs
- Upload statistics
- Cost tracking
- System health

---

## 🔌 **API Endpoints Needed**

### **Batch Processing**
- `POST /api/admin/bedrock/batch` - Create batch job
- `GET /api/admin/bedrock/batch/{jobId}` - Get job status
- `GET /api/admin/bedrock/batch` - List jobs
- `POST /api/admin/bedrock/batch/estimate` - Estimate cost
- `DELETE /api/admin/bedrock/batch/{jobId}` - Cancel job

### **Data Upload**
- `POST /api/admin/upload/presigned-url` - Get presigned URL
  - Input: `{ agency, date, fileName, fileSize }`
  - Output: `{ uploadUrl, s3Key, expiresAt }`
- `GET /api/admin/upload/history` - Get upload history

---

## 💻 **Implementation Plan**

### **Phase 1: API Endpoints**
1. Add presigned URL endpoint to admin API
2. Add batch processing endpoints to admin API
3. Test endpoints

### **Phase 2: Admin UI (sirsluginston-site)**
1. Add batch processing section
2. Add data upload section
3. Add dashboard

### **Phase 3: Admin UI (osha-trail)**
1. Create Admin page component
2. Add to routes
3. Copy batch processing components
4. Copy data upload components

---

## 📋 **Component Structure**

```
Admin.tsx
├── Dashboard Section
│   ├── Recent Jobs
│   ├── Upload Stats
│   └── Cost Summary
├── Batch Processing Section
│   ├── Filter Form
│   ├── Limit Controls
│   ├── Cost Estimator
│   └── Job List
└── Data Upload Section
    ├── File Upload
    ├── Agency/Date Selector
    ├── Upload Progress
    └── Upload History
```

---

## ✅ **Next Steps**

1. Create presigned URL API endpoint
2. Create batch processing API endpoints
3. Build Admin UI components
4. Add Admin page to OSHATrail
5. Test end-to-end

