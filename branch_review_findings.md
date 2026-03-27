# Branch Review: `feat/add-enriched-best-practices`

**Date:** 2026-03-27
**Base:** `main`
**Commits:** 2 (`2054cfa`, `b83bf90`)
**Files changed:** 6 (5 notebooks + 1 new markdown)
**Summary:** +2,947 / -2,429 lines

---

## Overview

This branch adds enriched BigQuery best practice checks across all five domain notebooks and introduces a new assessment reference document. The changes fall into three categories:

1. **New check sections** appended to notebooks 01-05
2. **A new reference document** (`06_best_practices_assessment/assessment_topics.md`)
3. **Unintentional reformatting** of notebook 04 (now reverted)

---

## Issues Found & Fixes Applied

### Critical (all fixed)

| # | Issue | Fix Applied |
|---|---|---|
| 1 | **`region-{REGION}` double-prefix bug** -- `REGION = "region-us"` + f-string `region-{REGION}` = `region-region-us` | Changed `REGION = "us"` in all notebooks. Setup queries converted to f-strings with `region-{REGION}`. |
| 2 | **Schema name mismatch** -- `CREATE SCHEMA bq_bestpractices_checklist` but view uses `bq_best_practices_checklist` | Standardized all references to `bq_best_practices_checklist` across all notebooks. |
| 3 | **`INTERVAL(30, DAY)` syntax** -- non-standard BigQuery SQL | Changed to `INTERVAL 30 DAY` in all setup queries. |
| 4 | **Undefined variables in 05 Tags section** -- `BILLING_PROJECT_ID`, etc. never defined | Added `BILLING_PROJECT_ID`, `BILLING_DATASET`, `BILLING_ACCOUNT_ID` to notebook 05 config cell. |

### Important (all fixed)

| # | Issue | Fix Applied |
|---|---|---|
| 5 | **Hardcoded `region-us`** in new sections | Replaced with `region-{REGION}` in all Python-based query cells across notebooks 01, 02, 03, 04. |
| 6 | **Hardcoded project `dataml-latam-argolis`** in `%%bigquery` magic cells | Replaced duplicate `%%bigquery` model creation cells (notebooks 02, 03) with reference to existing Section 0.1 parameterized model. |
| 7 | **Cost assumption mismatch** -- $5/TB (notebook 05) vs $6.25/TiB (notebook 03) | Standardized notebook 05 to $6.25/TiB (current BQ US multi-region On-Demand price). |
| 8 | **Notebook 04 reformatting** -- indentation change creating massive diff noise | Reverted to main branch formatting (2-space indent). New cells inserted before Summary section. |

### Minor (all fixed)

| # | Issue | Fix Applied |
|---|---|---|
| 9 | **Duplicate/out-of-order section numbering** | Renumbered all sections sequentially in notebooks 01-05. |
| 10 | **Duplicate Storage Billing Models check** in notebook 01 | Removed (overlapped with Section 1 org-level comparison). |
| 11 | **Duplicate MV Candidates check** in notebook 02 | Removed (overlapped with Section 2 Acceleration Candidates). |
| 12 | **Assessment doc out of date** | Updated `assessment_topics.md` with all checks from all notebooks. |
| 13 | **Duplicate recommendation markdown** in notebook 03 | Removed duplicate cell. |
| 14 | **Single-quote escaping bug** in notebook 03 Gemini prompt | Changed `replace("'", "\\'")` to `replace("'", "''")` for BigQuery SQL. |
| 15 | **Heading level inconsistency** in notebook 05 | Changed `###` to `##` for all check section headers. |
| 16 | **Undefined `df_recommendations` reference** in notebook 01 AI section | Removed the line referencing the undefined variable. |
| 17 | **Notebook 04 new cells placed after Summary** | Moved new cells (RLS/CLS, Authorized Views) before the Summary section. |

### Not fixed (pre-existing, out of scope)

| # | Issue | Reason |
|---|---|---|
| A | **`%%bigquery` cells in notebook 05** use hardcoded `region-us` | Pre-existing from main branch. `%%bigquery` magic doesn't support Python variables. Would require converting to Python client calls (larger refactor). |
| B | **Setup query + org-level view duplicated** across all 4 notebooks | Pre-existing design. Each notebook is intended to be self-contained/runnable independently. |
| C | **Notebook 04 uses `LOCATION` variable** instead of `REGION` | Pre-existing from main branch. Different variable naming convention in this notebook. |

---

## Final Section Structure

### 01. Ingestion & Storage
0. Pre-requirements | 1. Storage Model Optimization | 2. Data Layout | 3. Streaming Optimization | 4. AI-Powered Recommendations | 5. Time Travel & Fail-Safe | 6. Unused/Stale Tables

### 02. Processing & Performance
0. Pre-requirements | 1. Pruning Efficiency | 2. Acceleration Candidates | 3. Spill-to-Disk | 4. Anti-Patterns | 5. Google Cloud Recommendations | 6. Query Execution Plan | 7. Vector Search/BQML | 8. AI-Powered Recommendations

### 03. Resource Management
0. Pre-requirements | 1. On-Demand vs Slots | 2. Slot Contention | 3. Max Autoscaling | 4. Error Analysis | 5. Google Cloud Recommendations | 6. Editions Transition | 7. Workload Management | 8. AI-Powered Recommendations

### 04. Data Governance
(Pre-existing checks) + RLS/CLS Coverage | Authorized Views Validation | Summary

### 05. FinOps & Cost Allocation
0. Pre-requirements | 1. Billing Export Readiness | 2. Label Coverage | 3. Chargeback Modeling | 4. Commitment Gap | 5. Tags vs Labels | 6. Idle Projects | 7. AI-Powered Recommendations

---

*Generated by Claude Code on 2026-03-27*
