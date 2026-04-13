# Branch Review: `feat/add-enriched-best-practices` (Expert Review)

**Date:** 2026-03-27
**Base:** `main`
**Commits:** 4 (through `9c4a8d6`)
**Reviewer lens:** BigQuery INFORMATION_SCHEMA expert review

---

## Critical: Queries That Will Fail

### C1. `ROW_ACCESS_POLICIES` does not exist as an INFORMATION_SCHEMA view
**File:** `04_data_governance/04_data_governance.ipynb` (cell 52)

```sql
FROM `region-{LOCATION}`.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES
```

`INFORMATION_SCHEMA.ROW_ACCESS_POLICIES` **does not exist** in BigQuery at any level (dataset, project, or organization). Row access policies are only accessible via the REST API (`rowAccessPolicies.list`) or the `bq` CLI (`bq ls --row_access_policies`).

**Fix:** Replace with a Python API call:
```python
from google.cloud import bigquery
client = bigquery.Client()
dataset_ref = client.dataset(DATASET_ID)
policies = client.list_row_access_policies(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
```
Or iterate over tables in a dataset and call `list_row_access_policies()` for each.

---

### C2. Stale Tables query uses wrong column -- `creation_time` instead of `storage_last_modified_time`
**File:** `01_ingestion_storage/01_ingestion_storage.ipynb` (cell 197ed38d)

```sql
WHERE t.creation_time < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
```

This finds tables **created** over 90 days ago, NOT tables that haven't been **modified** in 90 days. A table created 2 years ago but actively written to daily would be flagged as "stale."

`INFORMATION_SCHEMA.TABLE_STORAGE` has the column `storage_last_modified_time` (TIMESTAMP) -- the most recent time data was written to the table. This is the correct column for staleness detection.

**Fix:** Use `TABLE_STORAGE_BY_PROJECT` directly (it already has all needed columns) and filter on `storage_last_modified_time`:
```sql
SELECT
  table_schema,
  table_name,
  storage_last_modified_time,
  SAFE_DIVIDE(total_logical_bytes, POW(1024, 3)) AS total_logical_gib,
  total_rows
FROM
  `region-{REGION}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT
WHERE
  deleted = false
  AND storage_last_modified_time < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  AND total_logical_bytes > 0
ORDER BY
  total_logical_bytes DESC
LIMIT 10;
```

---

### C3. Gemini prompt in notebook 01 doesn't escape single quotes
**File:** `01_ingestion_storage/01_ingestion_storage.ipynb` (cell 29cd3755)

```python
query_gemini = f"""
...
    (SELECT '{prompt}' AS prompt),
...
"""
```

If `prompt` contains an apostrophe (e.g., from dataset names like `user's_data`), this SQL will break with a syntax error. Notebooks 02 and 03 correctly use triple-quote escaping with `prompt.replace("'", "''")`.

**Fix:** Match the pattern used in other notebooks:
```python
query_gemini = f"""
...
    (SELECT '''{prompt.replace("'", "''")}''' AS prompt),
...
"""
```

---

## SQL Semantic Issues (Queries Run but Give Wrong/Misleading Results)

### S1. `slot_duration_ratio` measures parallelism, not inefficiency
**File:** `02_processing/02_processing.ipynb` (cell c4c472fd)

```sql
SAFE_DIVIDE(total_slot_ms, TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)) AS slot_duration_ratio
```

The markdown says: *"identifies jobs with heavy COMPUTE wait times or high data shuffling."*

This is incorrect. `slot_duration_ratio` = `total_slot_ms / wall_clock_ms` is the **average slot parallelism** -- how many slots were used concurrently on average. A value of 100 means 100 slots were running in parallel, which is normal and expected for large BigQuery queries. A high ratio indicates a query that successfully parallelized, NOT a bottleneck.

To actually detect compute bottlenecks, you would need to analyze `job_stages`:
```sql
SELECT
  job_id,
  stage.name,
  stage.wait_ms_avg,
  stage.compute_ms_avg,
  stage.read_ms_avg,
  stage.write_ms_avg,
  stage.shuffle_output_bytes_spilled
FROM `{PROJECT_ID}.bq_best_practices_checklist.jobs_by_top_20_projects`,
UNNEST(job_stages) AS stage
WHERE
  creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND stage.wait_ms_avg > stage.compute_ms_avg  -- Wait-bound stages
ORDER BY stage.wait_ms_avg DESC
LIMIT 20;
```

**Fix:** Either rewrite the query to analyze `job_stages` for wait/compute ratios, or update the markdown description to accurately describe what `slot_duration_ratio` measures (slot parallelism).

---

### S2. Vector Search section is discovery-only, not efficiency analysis
**File:** `02_processing/02_processing.ipynb` (cell f4e68ba5)

The section title is "Vector Search / BQML **Efficiency**" but the query just does `REGEXP_CONTAINS(UPPER(query), 'VECTOR_SEARCH')` to list queries containing the text "VECTOR_SEARCH". It doesn't assess:
- Whether vector indexes exist on the tables being searched
- Whether queries use brute-force vs. indexed search
- `total_bytes_processed` or `total_slot_ms` to measure actual resource cost
- Whether `distance_type` or `top_k` parameters are optimal

**Fix:** At minimum, add resource metrics to the query:
```sql
SELECT
  project_id, job_id, user_email, creation_time,
  total_bytes_processed,
  total_slot_ms,
  TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
  query
FROM {source_table}
WHERE
  creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND REGEXP_CONTAINS(UPPER(query), 'VECTOR_SEARCH')
  AND state = 'DONE'
ORDER BY total_slot_ms DESC
LIMIT 10;
```
And rename the section to "Vector Search / BQML Usage Discovery" if not adding index checks.

---

### S3. Missing `state = 'DONE'` filter on queries using `end_time`
**Files:**
- `02_processing/02_processing.ipynb` (cell c4c472fd) -- Query Execution Plan
- `03_resource_management/03_resource_management.ipynb` (cell 3a60eda6) -- Workload Management

Both queries calculate `TIMESTAMP_DIFF(end_time, start_time, ...)` without filtering for `state = 'DONE'`. For running or pending jobs, `end_time` is `NULL`, causing `TIMESTAMP_DIFF` to return `NULL`. While the `> 0` filter catches NULLs incidentally, `state = 'DONE'` is the explicit, correct filter.

**Fix:** Add `AND state = 'DONE'` to the WHERE clause of both queries.

---

### S4. Time Travel section shows all tables, not just those with significant time travel storage
**File:** `01_ingestion_storage/01_ingestion_storage.ipynb` (cell a461220d)

```sql
ORDER BY time_travel_physical_bytes DESC LIMIT 10;
```

No `WHERE` filter for `time_travel_physical_bytes > 0` or a meaningful threshold. Tables with zero time travel bytes will appear in results, adding noise. Also missing `deleted = false` filter.

**Fix:**
```sql
WHERE
  time_travel_physical_bytes > 0
  AND deleted = false
ORDER BY time_travel_physical_bytes DESC
LIMIT 10;
```

---

### S5. Chargeback markdown description inconsistent with code
**File:** `05_finops/05_finops.ipynb` (cell 194cf4f4, markdown)

> "We assume a standard on-demand rate ($5/TB) for estimation purposes"

But the code correctly uses `$6.25 per TiB`. The markdown description should match.

**Fix:** Update markdown to: *"We use the standard US multi-region on-demand rate ($6.25/TiB) for estimation."*

---

### S6. Idle Projects project-level fallback is functionally useless
**File:** `05_finops/05_finops.ipynb` (cell 5e4443a4)

When `USE_ORGANIZATION_VIEWS = False`, the fallback hard-codes `'{PROJECT_ID}'` as the project_id in both CTEs and checks if the current project has both storage and queries. Since you're running notebook queries in this project, it will almost always show up as "active" (not idle).

**Fix:** The project-level scope is inherently limiting for idle project detection (you can only see one project). Consider:
- Removing the fallback and noting this check requires org-level permissions
- Or checking at dataset granularity instead (idle datasets within the current project):
```sql
-- Datasets with storage but no recent query references
WITH StorageDatasets AS (
  SELECT DISTINCT table_schema
  FROM `region-{REGION}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT
  WHERE (total_logical_bytes > 0 OR total_physical_bytes > 0) AND deleted = false
),
QueriedDatasets AS (
  SELECT DISTINCT
    REGEXP_EXTRACT(referenced_table.table_id, r'^([^.]+)') AS dataset_id
  FROM `region-{REGION}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,
  UNNEST(referenced_tables) AS referenced_table
  WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
)
SELECT s.table_schema AS idle_dataset
FROM StorageDatasets s
LEFT JOIN QueriedDatasets q ON s.table_schema = q.dataset_id
WHERE q.dataset_id IS NULL;
```

---

### S7. Authorized Views section title says "Validation" but only does discovery
**File:** `04_data_governance/04_data_governance.ipynb` (cell 53-54)

The section lists all VIEWs in the project but doesn't verify if they are actually **authorized** on any dataset. Validating authorized views requires checking dataset access controls (via the BigQuery API's `dataset.get()` method, checking `access[].view` entries).

**Fix:** Rename to "View Discovery" or add actual authorization check via Python API:
```python
for dataset in client.list_datasets():
    ds = client.get_dataset(dataset.reference)
    for entry in ds.access_entries:
        if entry.entity_type == 'view':
            print(f"Authorized view: {entry.entity_id}")
```

---

## Pre-existing Issues Worth Noting

These issues exist on the `main` branch but are important for overall notebook quality:

### P1. `DATE_TRUNC` vs `TIMESTAMP_TRUNC` for minute-level truncation
**File:** `03_resource_management/03_resource_management.ipynb` (cell 00809637)

```sql
DATE_TRUNC(job_creation_time, MINUTE) AS minute_start
```

While BigQuery's `DATE_TRUNC` does support MINUTE with TIMESTAMP inputs (overloaded function), it returns a **DATE** type when used this way, losing the time component. This means all timestamps on the same calendar date would get the same truncated value, making the per-minute grouping incorrect.

Use `TIMESTAMP_TRUNC` instead:
```sql
TIMESTAMP_TRUNC(job_creation_time, MINUTE) AS minute_start
```

### P2. `CREATE SCHEMA` without location specification
**Files:** All notebooks (setup query)

```sql
CREATE SCHEMA IF NOT EXISTS bq_best_practices_checklist;
```

The schema is created without `OPTIONS(location='...')`. It will use the default location, which may not match `REGION`. Notebook 02's second setup cell correctly uses `OPTIONS(location='{REGION}')`.

**Fix:** Add location option:
```sql
CREATE SCHEMA IF NOT EXISTS bq_best_practices_checklist OPTIONS(location='region-{REGION}');
```

### P3. FinOps AI section uses mock output instead of actual Gemini call
**File:** `05_finops/05_finops.ipynb` (cell a6c12a41)

All other notebooks call Gemini via BQML `ML.GENERATE_TEXT`. This notebook just prints hardcoded placeholder text. It references `model_id = 'gemini_pro'` (undefined model) instead of the `bq_best_practices_checklist.gemini` model created in Section 0.1.

---

## Summary of Findings by Severity

| # | Severity | Notebook | Issue | New/Pre-existing |
|---|----------|----------|-------|------------------|
| C1 | **Critical** | 04 | `ROW_ACCESS_POLICIES` INFORMATION_SCHEMA view doesn't exist | New |
| C2 | **Critical** | 01 | Stale tables uses `creation_time` instead of `storage_last_modified_time` | New |
| C3 | **Critical** | 01 | Gemini prompt single quotes not escaped | Pre-existing |
| S1 | **Semantic** | 02 | `slot_duration_ratio` description is misleading (parallelism != inefficiency) | New |
| S2 | **Semantic** | 02 | Vector Search section is discovery only, not efficiency analysis | New |
| S3 | **Semantic** | 02, 03 | Missing `state = 'DONE'` filter on queries using `end_time` | New |
| S4 | **Semantic** | 01 | Time Travel shows all tables including those with 0 time travel bytes | New |
| S5 | **Semantic** | 05 | Chargeback markdown says $5/TB, code uses $6.25/TiB | Mixed |
| S6 | **Semantic** | 05 | Idle Projects project-level fallback is functionally useless | New |
| S7 | **Semantic** | 04 | Authorized Views "Validation" is just view listing | New |
| P1 | **Pre-existing** | 03 | `DATE_TRUNC` should be `TIMESTAMP_TRUNC` for MINUTE granularity | Pre-existing |
| P2 | **Pre-existing** | All | `CREATE SCHEMA` without location option | Pre-existing |
| P3 | **Pre-existing** | 05 | AI section uses mock output, not actual Gemini | Pre-existing |

---

*BigQuery Expert Review*
