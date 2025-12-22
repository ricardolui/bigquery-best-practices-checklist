# BigQuery Best Practices: Ingestion & Storage

**Objective:** Optimize how data is ingested and stored in BigQuery. We will analyze storage billing models, data layout (partitioning/clustering), and streaming efficiency.

**Contributors:**
*   Google Cloud Data Analytics Team


```python
# Initialize gcloud for authentication in the notebook, if running outside from BigQuery
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
REGION = "us" # UPDATE THIS to your dataset region (e.g., eu, us-central1)
GEMINI_MODEL_NAME = "gemini-3-pro-preview" # Updated to use gemini-3-pro-preview
VERTEX_AI_LOCATION = "global" # Vertex AI location for Gemini models
GEMINI_ENDPOINT_URL = f"https://aiplatform.googleapis.com/v1/projects/{PROJECT_ID}/locations/{VERTEX_AI_LOCATION}/publishers/google/models/{GEMINI_MODEL_NAME}"

print(f"Project: {PROJECT_ID}")
print(f"Region: {REGION}")
print(f"Gemini Endpoint: {GEMINI_ENDPOINT_URL}")
```

## 0. Pre-requirements

This section sets up the necessary prerequisites for the notebook.

### 0.1. Create BigQuery dataset

Create a BigQuery dataset 


```python
create_model_sql = f"""
CREATE SCHEMA IF NOT EXISTS `bq_bestpractices_checklist`
OPTIONS (location = '{REGION}')
"""

try:
    job = client.query(create_model_sql)
    job.result()
    print("Dataset created successfully.")
except Exception as e:
    print(f"Error creating model: {e}")

```

### 0.2. Create Gemini Model

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

### 0.3. Create Organization-level View

Create a dataset and a view that aggregates jobs from the top 20 projects in the organization. This view will be used as the source for the subsequent analysis.


```python
setup_query = """
EXECUTE IMMEDIATE (
  (
    SELECT
      'CREATE OR REPLACE VIEW `bq_bestpractices_checklist.jobs_by_top_20_projects` AS (' ||
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
  )
);
"""

try:
    job = client.query(setup_query)
    job.result()
    print("View created successfully.")
except Exception as e:
    print(f"Error creating view: {e}")
```

## 1. Storage Model Optimization

**Objective:** Identify datasets where switching between Logical and Physical storage billing models can save costs.
**Methodology:** Compare forecasted costs for both models based on active/long-term storage and compression ratios.
**Success Criteria:** Identify and migrate datasets with significant potential savings.


```python
query_storage = f"""
DECLARE active_logical_gib_price FLOAT64 DEFAULT 0.02;
DECLARE long_term_logical_gib_price FLOAT64 DEFAULT 0.01;
DECLARE active_physical_gib_price FLOAT64 DEFAULT 0.04;
DECLARE long_term_physical_gib_price FLOAT64 DEFAULT 0.02;

WITH
 storage_sizes AS (
   SELECT
     project_id   AS project_id,
     table_schema AS dataset_name,
     -- Logical
     SUM(IF(deleted=false, active_logical_bytes, 0)) / power(1024, 3) AS active_logical_gib,
     SUM(IF(deleted=false, long_term_logical_bytes, 0)) / power(1024, 3) AS long_term_logical_gib,
     -- Physical
     SUM(active_physical_bytes) / power(1024, 3) AS active_physical_gib,
     SUM(active_physical_bytes - time_travel_physical_bytes) / power(1024, 3) AS active_no_tt_physical_gib,
     SUM(long_term_physical_bytes) / power(1024, 3) AS long_term_physical_gib,
     -- Restorable previously deleted physical
     SUM(time_travel_physical_bytes) / power(1024, 3) AS time_travel_physical_gib,
     SUM(fail_safe_physical_bytes) / power(1024, 3) AS fail_safe_physical_gib,
   FROM
     `region-{REGION}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION
   WHERE total_physical_bytes + fail_safe_physical_bytes > 0
     -- Base the forecast on base tables only for highest precision results
     AND table_type  = 'BASE TABLE'
     GROUP BY 1,2
 )
SELECT
  project_id,
  dataset_name,
  -- Logical
  ROUND(active_logical_gib, 2) AS active_logical_gib,
  ROUND(long_term_logical_gib, 2) AS long_term_logical_gib,
  -- Physical
  ROUND(active_physical_gib, 2) AS active_physical_gib,
  ROUND(long_term_physical_gib, 2) AS long_term_physical_gib,
  ROUND(time_travel_physical_gib, 2) AS time_travel_physical_gib,
  ROUND(fail_safe_physical_gib, 2) AS fail_safe_physical_gib,
  -- Compression ratio
  ROUND(SAFE_DIVIDE(active_logical_gib, active_no_tt_physical_gib), 2) AS active_compression_ratio,
  ROUND(SAFE_DIVIDE(long_term_logical_gib, long_term_physical_gib), 2) AS long_term_compression_ratio,
  -- Forecast costs logical
  ROUND(active_logical_gib * active_logical_gib_price, 2) AS forecast_active_logical_cost,
  ROUND(long_term_logical_gib * long_term_logical_gib_price, 2) AS forecast_long_term_logical_cost,
  -- Forecast costs physical
  ROUND((active_no_tt_physical_gib + time_travel_physical_gib + fail_safe_physical_gib) * active_physical_gib_price, 2) AS forecast_active_physical_cost,
  ROUND(long_term_physical_gib * long_term_physical_gib_price, 2) AS forecast_long_term_physical_cost,
  -- Forecast costs total
  ROUND(((active_logical_gib * active_logical_gib_price) + (long_term_logical_gib * long_term_logical_gib_price)) -
     (((active_no_tt_physical_gib + time_travel_physical_gib + fail_safe_physical_gib) * active_physical_gib_price) + (long_term_physical_gib * long_term_physical_gib_price)), 2) AS forecast_total_cost_difference
FROM
  storage_sizes
ORDER BY
  (forecast_active_logical_cost + forecast_active_physical_cost) DESC;
"""

df_storage = client.query(query_storage).to_dataframe()
df_storage.head()
```


```python
# Filter for significant differences
df_significant_difference = df_storage[df_storage['forecast_total_cost_difference'].abs() > 0.00]
df_significant_difference = df_significant_difference.sort_values(by='forecast_total_cost_difference', ascending=False)
print("Datasets with potential cost differences (sorted by difference):")
df_significant_difference.head(10)
```


```python
recommendations = []

for index, row in df_significant_difference.iterrows():
    dataset_name = row['dataset_name']
    project_id = row['project_id']
    forecast_total_cost_difference = row['forecast_total_cost_difference']

    # Determine recommendation based on which model is cheaper
    # A positive forecast_total_cost_difference means logical is more expensive than physical
    # A negative forecast_total_cost_difference means physical is more expensive than logical
    if forecast_total_cost_difference > 0:
        recommended_billing_model = 'PHYSICAL'
    elif forecast_total_cost_difference < 0:
        recommended_billing_model = 'LOGICAL'
    else:
        recommended_billing_model = 'NO_CHANGE_COST_SAME'

    recommendations.append({
        'project_id' : project_id,
        'dataset_name': dataset_name,
        'recommended_billing_model': recommended_billing_model,
        'forecast_total_cost_difference': forecast_total_cost_difference
    })

df_storage_recommendations = pd.DataFrame(recommendations)
print("Recommended billing models for datasets:")
df_storage_recommendations.head()
```


```python
sql_commands = []

for index, row in df_storage_recommendations.iterrows():
    dataset_name = row['dataset_name']
    recommended_model = row['recommended_billing_model']
    project = row['project_id']

    if recommended_model in ['LOGICAL', 'PHYSICAL']:
        command = f"ALTER SCHEMA `{project}.{dataset_name}` SET OPTIONS (storage_billing_model = '{recommended_model}');"
        sql_commands.append(command)

print("--Generated SQL commands for manual execution:\n")
for cmd in sql_commands:
    print(cmd)
```

**Recommendation:** Review the generated `ALTER SCHEMA` commands and execute them to switch billing models for cost savings.

**WARNING:** If you would like to implement the previous suggestions, run the next cell


```python
from google.cloud import bigquery

print("Generated SQL commands for manual execution:\n")
for cmd in sql_commands:
    print(cmd)

# Executes the commands
# Initialize BigQuery Client (ensure this is done once, but re-init here for self-contained cell)
try:
    client
except NameError:
    client = bigquery.Client()

print("Executing SQL commands dynamically...")
for i, cmd in enumerate(sql_commands):
    print(f"\nExecuting command {i+1}/{len(sql_commands)}: {cmd}")
    try:
        query_job = client.query(cmd)
        query_job.result() # Waits for the job to complete
        print(f"Command {i+1} executed successfully.")
    except Exception as e:
        print(f"Error executing command {i+1}: {e}")

print("\nDynamic execution complete.")
```

## 2. Data Layout: Detecting Date-Sharded Tables

**Objective:** Identify legacy date-sharded tables (e.g., `events_20240101`).
**Methodology:** Regex match on table names in `TABLE_STORAGE_BY_ORGANIZATION`.
**Success Criteria:** Migrate these tables to **Native Partitioning** for better performance and management.


```python
query_sharding = f"""
SELECT
    table_catalog as project_id,
    table_schema as dataset_id,
    REGEXP_EXTRACT(table_name, r'(.*)_\\d{{8}}$') AS base_table_name,
    COUNT(*) as shard_count,
    SUM(total_rows) as total_rows,
    SUM(total_logical_bytes) / 1024 / 1024 / 1024 as total_logical_gb
FROM
    `region-{REGION}.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION`
WHERE
    REGEXP_CONTAINS(table_name, r'_\\d{{8}}$')
GROUP BY
    1, 2, 3
HAVING
    shard_count > 5
ORDER BY
    total_logical_gb DESC;
"""

df_sharding = client.query(query_sharding).to_dataframe()
print("Top Date-Sharded Tables Candidates for Partitioning:")
df_sharding.head()
```

**Recommendation:** Convert sharded tables to partitioned tables to improve query performance and reduce metadata overhead.

## 3. Streaming Optimization

**Objective:** Detect high usage of legacy streaming inserts.
**Methodology:** Analyze `STREAMING_TIMELINE_BY_ORGANIZATION` for legacy requests.
**Success Criteria:** Migrate to **Storage Write API** for lower costs and higher throughput.


```python
query_streaming = f"""
SELECT
  project_id,
  dataset_id,
  table_id,
  SUM(total_requests) AS total_legacy_requests,
  SUM(total_input_bytes) AS total_legacy_bytes
FROM
  `region-{REGION}`.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_ORGANIZATION
WHERE
  start_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY
  1, 2, 3
ORDER BY
  total_legacy_bytes DESC;
"""

df_streaming = client.query(query_streaming).to_dataframe()
print("Top Tables using Legacy Streaming:")
df_streaming.head()
```

**Recommendation:** If `total_legacy_bytes` is high, consider switching to the BigQuery Storage Write API.

## 4. AI-Powered Recommendations

**Objective:** Use Generative AI to synthesize findings.
**Methodology:** Summarize findings and use Gemini Pro via BQML.


```python
import json
from IPython.display import display, Markdown

# Summarize findings for Gemini
summary_text = f"""
Audit Findings for Ingestion & Storage:
1. Storage Model: Found {len(df_storage_recommendations)} datasets where switching billing models could save costs.
2. Data Layout: Identified {len(df_sharding)} date-sharded tables that should be partitioned.
3. Streaming: Found {len(df_streaming)} tables using legacy streaming inserts.
"""

# Construct the prompt
prompt = f"Analyze the following metrics regarding BigQuery Ingestion & Storage and provide 3 actionable recommendations. Context: {summary_text}"

# Call Gemini via BQML
query_gemini = f"""
SELECT
  ml_generate_text_result['candidates'][0]['content'] AS recommendation
FROM
  ML.GENERATE_TEXT(
    MODEL `bq_bestpractices_checklist.gemini`,
    (SELECT '''{prompt}''' AS prompt),
    STRUCT(
      0.2 AS temperature,
      8192 AS max_output_tokens
    )
  )
"""

try:
    df_gemini = client.query(query_gemini).to_dataframe()
    print("AI-Powered Recommendations:")
    for index, row in df_gemini.iterrows():
        try:
          # Parse the JSON string from Gemini
          recommendation_data = json.loads(row['recommendation'])
          
          # Extract the text content
          if 'parts' in recommendation_data and recommendation_data['parts']:
              text_content = recommendation_data['parts'][0]['text']
              display(Markdown(text_content))
          else:
              # Fallback if structure is different
              print(row['recommendation'])
                
        except json.JSONDecodeError:
            # Fallback if not valid JSON
            print(row['recommendation'])
        except Exception as e:
            print(f"Error displaying recommendation: {e}")
except Exception as e:
    print("Error calling Gemini. Ensure the model exists and you have permissions.", e)
```
