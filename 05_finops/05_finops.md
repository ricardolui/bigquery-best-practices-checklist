# BigQuery Best Practices: FinOps & Cost Allocation

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

This notebook implements the technical validation for the **FinOps & Cost Allocation** pillar of the BigQuery Health Check.

### Key Focus Areas:
1.  **Billing Export Readiness**: Check if a dataset specifically for billing export exists.
2.  **Label Coverage Analysis**: Calculate the percentage of slots and storage bytes that are "Unattributed" (missing labels).
3.  **Chargeback Modeling**: Simulate a chargeback invoice by aggregating costs by User/Principal.
4.  **Commitment Gap Analysis**: Compare average slot usage vs. purchased commitments to identify over-provisioning or opportunities for CUDs.
5.  **AI-Powered Recommendations**: Generate actionable insights using Gemini.

### Prerequisites
*   Permissions to view `INFORMATION_SCHEMA.JOBS_BY_PROJECT` and `INFORMATION_SCHEMA.CAPACITY_COMMITMENTS`.
*   BigQuery API enabled.
*   Vertex AI API enabled (for Gemini recommendations).


### 1. Billing Export Readiness
Check if a dataset specifically for billing export exists. If not, we provide instructions.


```python
%%bigquery billing_datasets_df
SELECT
    schema_name,
    creation_time,
    location
FROM
    INFORMATION_SCHEMA.SCHEMATA
WHERE
    schema_name LIKE '%billing%'
    OR schema_name LIKE '%export%'
```


```python
if billing_datasets_df.empty:
    print("WARNING: No obvious billing export datasets found (searching for 'billing' or 'export' in name).")
    print("Recommendation: Enable Cloud Billing export to BigQuery for granular cost analysis.")
    print("Instructions: https://cloud.google.com/billing/docs/how-to/export-data-bigquery")
else:
    print("Potential billing datasets found:")
    print(billing_datasets_df)
```


### 2. Label Coverage Analysis
Calculate the percentage of slots and storage bytes that are "Unattributed" (missing labels). This helps in identifying how much of the workload is not being tracked for chargeback.


```python
%%bigquery label_coverage_df
WITH JobLabels AS (
    SELECT
        job_id,
        total_bytes_billed,
        total_slot_ms,
        (SELECT COUNT(*) FROM UNNEST(job_labels)) AS label_count
    FROM
        `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT -- Adjust region if necessary
    WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    )
SELECT
    COUNT(*) AS total_jobs,
    SUM(CASE WHEN label_count = 0 THEN 1 ELSE 0 END) AS jobs_without_labels,
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN label_count = 0 THEN total_bytes_billed ELSE 0 END), SUM(total_bytes_billed)) * 100, 2) AS pct_bytes_unlabeled,
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN label_count = 0 THEN total_slot_ms ELSE 0 END), SUM(total_slot_ms)) * 100, 2) AS pct_slots_unlabeled
FROM
    JobLabels
```


```python
# Display Label Coverage
print("Label Coverage Analysis (Last 30 Days):")
display(label_coverage_df)

if not label_coverage_df.empty and label_coverage_df['pct_bytes_unlabeled'].iloc[0] > 20:
    print("\nALERT: Significant portion of bytes billed (>20%) is unlabeled. Implement a mandatory labeling policy.")
```


### 3. Chargeback Modeling
Simulate a chargeback invoice by aggregating costs by User/Principal. We assume a standard on-demand rate ($5/TB) for estimation purposes if pricing data isn't directly available.


```python
%%bigquery chargeback_df
SELECT
    user_email,
    COUNT(job_id) as job_count,
    ROUND(SUM(total_bytes_billed) / POW(1024, 4) * 5, 2) AS estimated_cost_usd, -- $5 per TB assumption
    ROUND(SUM(total_slot_ms) / (1000 * 60 * 60), 2) AS total_slot_hours
FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
    creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY
    1
ORDER BY
    estimated_cost_usd DESC
LIMIT 10
```


```python
# Visualize Top Spenders
plt.figure(figsize=(10, 6))
sns.barplot(data=chargeback_df, x='estimated_cost_usd', y='user_email')
plt.title('Top 10 Users by Estimated Cost (Last 30 Days)')
plt.xlabel('Estimated Cost (USD)')
plt.ylabel('User Email')
plt.show()
```


### 4. Commitment Gap Analysis
Compare average slot usage vs. purchased commitments to identify over-provisioning or opportunities for CUDs.


```python
%%bigquery commitment_gap_df
WITH Usage AS (
    SELECT
        TIMESTAMP_TRUNC(creation_time, HOUR) as usage_hour,
        SUM(total_slot_ms) / (1000 * 60 * 60) as slot_hours_used
    FROM
        `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE
        creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY 1
),
Commitments AS (
    SELECT
        SUM(slot_count) as total_slots_committed
    FROM
        `region-us`.INFORMATION_SCHEMA.CAPACITY_COMMITMENTS
    WHERE
        state = 'ACTIVE'
)
SELECT
    u.usage_hour,
    u.slot_hours_used,
    c.total_slots_committed,
    (c.total_slots_committed - u.slot_hours_used) as unused_slots
FROM
    Usage u
CROSS JOIN
    Commitments c
ORDER BY
    u.usage_hour
```


```python
if not commitment_gap_df.empty:
    plt.figure(figsize=(12, 6))
    plt.plot(commitment_gap_df['usage_hour'], commitment_gap_df['slot_hours_used'], label='Slot Usage')
    plt.axhline(y=commitment_gap_df['total_slots_committed'].iloc[0], color='r', linestyle='--', label='Committed Slots')
    plt.title('Slot Usage vs Commitment (Last 7 Days)')
    plt.xlabel('Time')
    plt.ylabel('Slots')
    plt.legend()
    plt.show()
else:
    print("No usage data found for commitment gap analysis.")
```


### 5. AI-Powered Recommendations
Using Gemini to analyze the gathered metrics and provide actionable recommendations.


```python
# Prepare summary string for Gemini
summary_text = f"""
Analyze the following metrics regarding FinOps & Cost Allocation:
1. Label Coverage: {label_coverage_df.to_dict('records') if not label_coverage_df.empty else 'No data'}
2. Top Spenders: {chargeback_df.head(3).to_dict('records') if not chargeback_df.empty else 'No data'}
3. Commitment Gap: {commitment_gap_df.describe().to_dict() if not commitment_gap_df.empty else 'No data'}

Provide 3 actionable recommendations on topic FinOps & Cost Allocation.
"""

# Call Gemini (assuming a remote model 'gemini_pro' exists in dataset 'ml_dataset')
# Note: You need to replace 'your_project.ml_dataset.gemini_pro' with your actual model path
model_id = 'gemini_pro' # Placeholder, user should update

print("Sending the following context to Gemini for analysis:")
print(summary_text)

# In a real scenario, you would run:
# df_result = client.query(f"SELECT ml_generate_text_result FROM ML.GENERATE_TEXT(MODEL `{model_id}`, (SELECT '{summary_text}' AS prompt))").to_dataframe()
# print(df_result['ml_generate_text_llm_result'].iloc[0])

print("\n[Mock Output] Gemini Recommendations:")
print("1. Implement a strict tagging policy to reduce unattributed costs.")
print("2. Review top spenders and optimize their most expensive queries using partitioning/clustering.")
print("3. Consider purchasing Committed Use Discounts (CUDs) to cover the baseline slot usage observed.")
```

