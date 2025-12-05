# BigQuery Best Practices: Processing & Performance

**Objective:** Analyze the efficiency of your BigQuery workloads. We will look for queries that scan too much data (poor pruning), consume excessive resources (high slot usage), spill to disk (memory issues), or use common anti-patterns.

**Contributors:**
*   Google Cloud Data Analytics Team


```python
# Initialize gcloud for authentication in the notebook
!gcloud auth application-default login
```


```python
import pandas as pd
import bigframes.pandas as bpd
from google.cloud import bigquery
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize BigQuery Client
client = bigquery.Client()

# Configuration
PROJECT_ID = client.project  # Uses default project from environment
REGION = "region-us" # UPDATE THIS to your dataset region (e.g., region-eu, region-us-central1)
GEMINI_MODEL_NAME = "gemini-3-pro-preview" # Updated to use gemini-3-pro-preview
VERTEX_AI_LOCATION = "global" # Vertex AI location for Gemini models
GEMINI_ENDPOINT_URL = f"https://aiplatform.googleapis.com/v1/projects/{PROJECT_ID}/locations/{VERTEX_AI_LOCATION}/publishers/google/models/{GEMINI_MODEL_NAME}"

print(f"Project: {PROJECT_ID}")
print(f"Region: {REGION}")
print(f"Gemini Endpoint: {GEMINI_ENDPOINT_URL}")
```


## 0. Pre-requirements

This section sets up the necessary prerequisites for the notebook.


### 0.1. Create Gemini Model

Create a BQML Remote Model that uses the Gemini model via DEFAULT connection for AI-powered recommendations.


```python
create_model_sql = f"""
CREATE OR REPLACE MODEL `bq_bestpractices_checklist.gemini`
REMOTE WITH CONNECTION DEFAULT
OPTIONS (endpoint = '{GEMINI_ENDPOINT_URL}')
"""


try:
    job = client.query(create_model_sql)
    job.result()
    print("Model created successfully.")
except Exception as e:
    print(f"Error creating model: {e}")

```


### 0.2. Create Organization-level View

Create a dataset and a view that aggregates jobs from the top 20 projects in the organization. This view will be used as the source for the subsequent analysis.


```python
setup_query = """
CREATE SCHEMA IF NOT EXISTS bq_bestpractices_checklist;

EXECUTE IMMEDIATE (
  (
    SELECT
      'CREATE OR REPLACE VIEW `bq_best_practices_checklist.jobs_by_top_20_projects` AS (' ||
      STRING_AGG(
        'SELECT * FROM `' || project_id || '.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`',
        ' UNION ALL '
      ) || ')'
    FROM (
      SELECT
        project_id
      FROM
        `region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`
      WHERE 
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL(30, DAY))
      GROUP BY
        1
      ORDER BY
        SUM(total_slot_ms) DESC
      LIMIT
        20
    )
  )
);
"""

try:
    job = client.query(setup_query)
    job.result()
    print("Dataset and View created successfully.")
except Exception as e:
    print(f"Error creating view: {e}")
```


```python
# First, ensure the schema exists
create_schema_query = f"CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}`.bq_bestpractices_checklist OPTIONS(location='{REGION}');"
try:
    print(f"Attempting to create schema: {create_schema_query}")
    job = client.query(create_schema_query)
    job.result()
    print("Schema created or already exists successfully.")
except Exception as e:
    print(f"Error creating schema: {e}")
    # If schema creation fails, the view creation will also fail, so we might as well stop here.
    raise

# Then, create the view
setup_view_query = f'''
EXECUTE IMMEDIATE(
    SELECT
      'CREATE OR REPLACE VIEW `{PROJECT_ID}.bq_bestpractices_checklist.jobs_by_top_20_projects` AS (' ||
      STRING_AGG(
        'SELECT * FROM `' || project_id || '.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`',
        ' UNION ALL '
      ) || ')'
    FROM (
      SELECT
        project_id
      FROM
        `region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`
      WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
      GROUP BY
        1
      ORDER BY
        SUM(total_slot_ms) DESC
      LIMIT
        20
    )
);
'''

try:
    print(f"Attempting to create view.")
    job = client.query(setup_view_query)
    job.result()
    print("View created successfully.")
except Exception as e:
    print(f"Error creating view: {e}")
```


## 1. Pruning Efficiency

**Objective:** Detect lack of partition filters.
**Methodology:** Calculate the "Scan Efficiency Ratio" (Bytes Billed / Bytes Processed).
**Success Criteria:** A ratio close to 1 is ideal. A high ratio (e.g., > 10) suggests the query is scanning far more data than it actually needs, often due to missing partition or clustering filters.


```python
query_pruning = f"""
SELECT
  job_id,
  creation_time,
  user_email,
  total_bytes_billed,
  total_bytes_processed,
  SAFE_DIVIDE(total_bytes_billed, total_bytes_processed) AS scan_efficiency_ratio,
  query
FROM
 `{PROJECT_ID}.bq_bestpractices_checklist.jobs_by_top_20_projects`
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND total_bytes_processed > 0
  AND job_type = 'QUERY'
  -- Filter for significant queries to avoid noise
  AND total_bytes_billed > 1024*1024*100 -- > 100MB
ORDER BY
  scan_efficiency_ratio DESC
LIMIT 20
"""

df_pruning = client.query(query_pruning).to_dataframe()
print("Top 20 Queries with Poor Pruning Efficiency:")
df_pruning.head()
```


**Recommendation:** Queries with high Scan Efficiency Ratio should be reviewed to ensure they use Partition/Clustering keys in the WHERE clause.


## 2. Acceleration Candidates

**Objective:** Identify queries that would benefit from **BigQuery Materialized Views** or **BI Engine**.
**Methodology:** Look for repeated queries with high aggregation costs (Slot Time) relative to data size.
**Success Criteria:** High-computation, repeated queries are moved to Materialized Views to save costs and improve latency.


```python
query_acceleration = f"""
SELECT
  query_info.query_hashes.normalized_literals AS query_hash,
  COUNT(*) AS execution_count,
  AVG(total_slot_ms) AS avg_slot_ms,
  AVG(total_bytes_processed) AS avg_bytes_processed,
  SUM(total_slot_ms) / 1000 / 3600 AS total_slot_hours,
  ANY_VALUE(query) as sample_query
FROM
  `{PROJECT_ID}.bq_bestpractices_checklist.jobs_by_top_20_projects`
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
  AND cache_hit = FALSE
GROUP BY
  1
HAVING
  execution_count > 5 -- Repeated at least 5 times
ORDER BY
  total_slot_hours DESC
LIMIT 20
"""

df_acceleration = client.query(query_acceleration).to_dataframe()
print("Top Candidates for Acceleration (High Slot Usage & Repetition):")
df_acceleration.head()
```


**Recommendation:** If 'execution_count' is high and 'avg_slot_ms' is significant, consider creating a Materialized View for the common aggregation logic.


## 3. Spill-to-Disk Events

**Objective:** Detect stages writing to disk, indicating memory issues.
**Methodology:** Analyze `shuffle_output_bytes_spilled` in job stages.
**Success Criteria:** Zero bytes spilled to disk. Spilling slows down queries significantly.
**Remediation:** Adjust clustering keys to reduce shuffle size, or refactor logic (e.g., avoid massive `GROUP BY` on high-cardinality fields).


```python
query_spill = f"""
SELECT
  job_id,
  creation_time,
  user_email,
  total_slot_ms,
  (SELECT SUM(shuffle_output_bytes_spilled) FROM UNNEST(job_stages)) / 1024 / 1024 / 1024 AS total_gb_spilled,
  query
FROM
 `{PROJECT_ID}.bq_bestpractices_checklist.jobs_by_top_20_projects`
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND EXISTS (SELECT 1 FROM UNNEST(job_stages) WHERE shuffle_output_bytes_spilled > 0)
ORDER BY
  total_gb_spilled DESC
LIMIT 20
"""

df_spill = client.query(query_spill).to_dataframe()
print("Top Queries with Spill-to-Disk Events:")
df_spill.head()
```


**Recommendation:** For queries spilling to disk, check if the JOIN keys or GROUP BY keys are skewed. Consider using INT64 keys instead of STRING where possible.


## 4. Anti-Patterns

**Objective:** Identify complex SQL anti-patterns using the BigQuery Anti-Pattern Recognition Tool.
**Methodology:** Use the `get_antipatterns` UDF to analyze query text.
**Prerequisite:** You must install the UDF in your project. Follow the instructions below.

1.  Clone the repository: `git clone https://github.com/GoogleCloudPlatform/bigquery-antipattern-recognition.git`
2.  Navigate to the Terraform directory: `cd bigquery-antipattern-recognition/udf/terraform`
3.  Initialize and apply Terraform: `terraform init && terraform apply`

**Success Criteria:** Minimal occurrence of these patterns in production pipelines.


```python
query_antipatterns = f"""
SELECT
  job_id,
  creation_time,
  user_email,
  query,
  `{PROJECT_ID}.fns.get_antipatterns`(query) AS anti_patterns
FROM
  `{PROJECT_ID}.bq_bestpractices_checklist.jobs_by_top_20_projects`
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
  -- Filter out simple queries or scripts if needed
LIMIT 100
"""

try:
    df_antipatterns = client.query(query_antipatterns).to_dataframe()
    # Filter rows where anti-patterns were found (assuming UDF returns null or empty list/struct for no matches)
    # Adjust filtering logic based on UDF actual return type
    print("Queries with Detected Anti-Patterns:")
    df_antipatterns.head()
except Exception as e:
    print("Error running Anti-Pattern query. Ensure the UDF is installed via Terraform.")
    print(e)
```


**Recommendation:** Review the `anti_patterns` column for specific advice on how to optimize the query. Common issues include `SELECT *`, `ORDER BY` without `LIMIT`, and inefficient JOINs.


## 5. Google Cloud Recommendations

**Objective:** Review automated recommendations provided by Google Cloud for cost and performance optimization.
**Methodology:** Query `INFORMATION_SCHEMA.RECOMMENDATIONS_BY_ORGANIZATION` to find potential slot and storage savings.
**Success Criteria:** Regular review and application of high-impact recommendations.


```python
query_recommendations = f"""
SELECT
  project_id,
  recommender,
  subtype,
  LAX_INT64(additional_details.overview.bytesSavedMonthly) / POW(1024, 3) as est_gb_saved_monthly,
  LAX_INT64(additional_details.overview.slotMsSavedMonthly) / (1000 * 3600) as slot_hours_saved_monthly,
  last_updated_time
FROM
 `region-us`.INFORMATION_SCHEMA.RECOMMENDATIONS_BY_ORGANIZATION
ORDER BY
  est_gb_saved_monthly DESC
LIMIT 20
"""

try:
    df_recommendations = client.query(query_recommendations).to_dataframe()
    print("Top Google Cloud Recommendations:")
    df_recommendations.head()
except Exception as e:
    print("Error querying Recommendations. Ensure you have the necessary permissions (Recommender Viewer) and the region is correct.", e)
    df_recommendations = pd.DataFrame() # Empty DF for summary
```


**Recommendation:** Prioritize recommendations with high `est_gb_saved_monthly` or `slot_hours_saved_monthly`. Investigate the specific resources mentioned in the recommendation details.


## 6. AI-Powered Recommendations

**Objective:** Use Generative AI to synthesize the findings and provide actionable advice.
**Methodology:** Summarize the dataframes above and pass them to a Gemini Pro model via BQML.


```python
%%bigquery
CREATE OR REPLACE MODEL `bq_bestpractices_checklist.gemini`
REMOTE WITH CONNECTION `us.llm`
OPTIONS (endpoint = 'https://aiplatform.googleapis.com/v1/projects/dataml-latam-argolis/locations/global/publishers/google/models/gemini-3-pro-preview')
```


```python
# Summarize findings for Gemini
summary_text = f'''
Audit Findings for Processing Layer & Performance:
1. Pruning Efficiency: Found {len(df_pruning)} queries with poor scanning efficiency (high Bytes Billed / Bytes Processed).
2. Acceleration: Identified {len(df_acceleration)} repeated heavy queries suitable for Materialized Views.
3. Spills: Detected {len(df_spill)} queries spilling to disk (memory pressure).
4. Anti-Patterns: Flagged {len(df_antipatterns)} queries with anti-patterns (using Anti-Pattern Recognition Tool).
5. Google Cloud Recommendations: Found {len(df_recommendations)} active recommendations from the platform.
'''

# Construct the prompt
prompt = f"Analyze the following metrics regarding Processing Layer & Performance and provide 3 actionable recommendations on topic Processing Layer & Performance. Context: {summary_text}"

# Call Gemini via BQML
query_gemini = f"""
SELECT
  ml_generate_text_result['candidates'][0]['content'] AS recommendation
FROM
  ML.GENERATE_TEXT(
    MODEL `{PROJECT_ID}.bq_bestpractices_checklist.gemini`,
    (SELECT '''{prompt.replace("'", "''")}''' AS prompt),
    STRUCT(
      0.2 AS temperature,
      8192 AS max_output_tokens
    )
  )
"""

try:
    df_gemini = client.query(query_gemini).to_dataframe()
    print("Gemini Recommendations:")
    print(df_gemini['recommendation'].iloc[0])
except Exception as e:
    print("Error calling Gemini Model (Ensure Model and Connection exist):", e)
    print("Prompt used:", prompt)
```

