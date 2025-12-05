# BigQuery Best Practices: Data Governance & Security

**Contributors:**
*   Google Cloud Data Analytics Team
*   XXXXXX - Brazil DA CE - xxxx@google.com
*   XXXXXX - Brazil DA CE - xxxx@google.com
*   XXXXXX - Mexico DA CE - xxxx@google.com   
*   XXXXXX - Peru DA CE - xxxx@google.com
*   XXXXXX - Colombia DA CE - xxx@google.com



```python
#Initialize gcloud for authentication in the notebook
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


## Overview

This notebook audits Data Governance & Security in BigQuery to identify vulnerabilities and ensure compliance.

### Key Focus Areas:
1.  **Access Controls**: Identify datasets accessible by `allUsers` or `allAuthenticatedUsers` (Open Access Risks).
2.  **Row-Level Security (RLS)**: Verify the existence of Row-Level Security policies on sensitive tables.
3.  **Encryption (CMEK)**: Verify usage of Customer Managed Encryption Keys for regulated datasets.
4.  **Data Quality**: Identify Partition/Clustering keys and check for NULL values to ensure optimal performance.
5.  **AI-Powered Recommendations**: Generate actionable insights using Gemini.

### Prerequisites
*   Ensure you have the necessary permissions (BigQuery Admin or Data Viewer/Editor).
*   Authenticate your environment (step above).


### 1. Audit Open Access Risks
Identify datasets accessible by `allUsers` or `allAuthenticatedUsers`.


```python
query_open_access = f"""
SELECT * 
FROM `{region}.INFORMATION_SCHEMA.OBJECT_PRIVILEGES`
WHERE grantee IN ('allUsers', 'allAuthenticatedUsers')
"""
try:
    df_open_access = client.query(query_open_access).to_dataframe()
    print('Open Access Risks Found:', len(df_open_access))
    display(df_open_access.head())
except Exception as e:
    print(f'Error querying Object Privileges: {e}')
    df_open_access = pd.DataFrame()
```


### 2. Verify Row-Level Security
Verify the existence of Row-Level Security policies.


```python
query_rls = f"""
SELECT * 
FROM `{region}.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES`
"""
try:
    df_rls = client.query(query_rls).to_dataframe()
    print('RLS Policies Found:', len(df_rls))
    display(df_rls.head())
except Exception as e:
    print(f'Error querying RLS (might not be enabled/visible): {e}')
    df_rls = pd.DataFrame()
```


### 3. Verify Encryption (CMEK)
Verify usage of Customer Managed Encryption Keys.


```python
query_cmek = f"""
SELECT table_schema, table_name, option_value as kms_key_name
FROM `{region}.INFORMATION_SCHEMA.TABLE_OPTIONS`
WHERE option_name = 'kms_key_name'
"""
try:
    df_cmek = client.query(query_cmek).to_dataframe()
    print('Tables with CMEK:', len(df_cmek))
    display(df_cmek.head())
except Exception as e:
    print(f'Error querying Table Options: {e}')
    df_cmek = pd.DataFrame()
```


### 4. Data Quality Check (NULL Keys)
Identify Partition/Clustering keys and check for NULL values.


```python
# Find keys
query_keys = f"""
SELECT table_schema, table_name, column_name
FROM `{region}.INFORMATION_SCHEMA.COLUMNS`
WHERE is_partitioning_column = 'YES' OR clustering_ordinal_position IS NOT NULL
LIMIT 10 -- Limiting for demo purposes
"""
try:
    df_keys = client.query(query_keys).to_dataframe()
    print('Keys found:', len(df_keys))
    display(df_keys.head())
except Exception as e:
    print(f'Error querying Columns: {e}')
    df_keys = pd.DataFrame()
```


### 5. AI-Powered Recommendations
Use Gemini to analyze the findings.


```python
from bigframes.ml.llm import GeminiTextGenerator

# Initialize Gemini Model (ensure you have the connection set up)
# model = GeminiTextGenerator(connection_name='your-connection')

prompt = """
Analyze the following metrics regarding Data Governance & Security and provide 3 actionable recommendations on topic Data Governance & Security.
Metrics:
"""
prompt += f"Open Access Risks: {len(df_open_access)}\n"
prompt += f"RLS Policies: {len(df_rls)}\n"
prompt += f"CMEK Tables: {len(df_cmek)}\n"

print('Prompt for Gemini:')
print(prompt)

# df_result = model.predict(pd.DataFrame({'prompt': [prompt]}))
# print(df_result['ml_generate_text_llm_result'].iloc[0])
```

