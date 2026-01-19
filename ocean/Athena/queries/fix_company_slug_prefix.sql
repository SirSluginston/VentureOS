-- FIX: Remove SLUG# or COMPANY# prefix from company_slug in Silver
-- The processor was storing the full DynamoDB key instead of just the slug

UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET company_slug = REGEXP_REPLACE(company_slug, '^(COMPANY|SLUG)#', '')
WHERE company_slug IS NOT NULL 
  AND REGEXP_LIKE(company_slug, '^(COMPANY|SLUG)#');

