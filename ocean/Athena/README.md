# The Athena Compactor
# ====================

## Overview
This folder contains the SQL queries and orchestration scripts for the **Phase 3: The Ocean** component.
Athena acts as the **Compactor Engine**, merging small staging files (The Tributary) into the main Iceberg tables (The Deep).

## Structure
- `queries/`: Raw SQL for creating tables and merging data.
- `scripts/`: Node.js scripts to trigger Athena queries via EventBridge or Cron.

## Workflows
1.  **Initialize:** Create the Iceberg Tables (`create-tables.sql`).
2.  **Daily Merge:** Run the `MERGE INTO` statement to deduplicate and compact data (`merge-data.sql`).

