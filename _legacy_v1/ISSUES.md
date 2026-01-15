# VentureOS Issues & Technical Debt

## Active Issues

### 1. Bedrock Pipeline Debug
**Description**: Content generation showing "pending" instead of AI-generated text  
**Impact**: Users see raw government text  
**Priority**: High  
**Next Steps**: Check CloudWatch logs, verify Bedrock API calls, test overlay retrieval

### 2. Minimal Data Imported
**Description**: Only ~101k OSHA violations (SIR dataset)  
**Impact**: Limited SEO potential  
**Priority**: High  
**Next Steps**: Import full OSHA enforcement data, add MSHA, then new agencies

### 3. AWS IAM Policies Need Audit
**Description**: Manually managed policies in `iam/` folder  
**Impact**: Potential security gaps, not following least privilege  
**Priority**: Medium  
**Next Steps**: Full AWS audit after more data/features implemented

## Technical Debt

### Performance
- [ ] Pre-aggregate top 1,000 companies (CompaniesTrail optimization)
- [ ] Implement Redis caching (faster than DynamoDB for hot data)
- [ ] Optimize Athena queries with materialized views

### Monitoring
- [ ] Set up cost alerts ($100/month threshold)
- [ ] Lambda execution time dashboards
- [ ] Error rate tracking per function

### Security
- [ ] Move secrets to AWS Secrets Manager
- [ ] Implement API key rotation
- [ ] Add WAF rules (DDoS protection)

---

**Last Updated**: January 10, 2026

