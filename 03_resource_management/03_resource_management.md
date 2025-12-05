# BigQuery Best Practices: Resource Management

**Objective:** Identify bottlenecks caused by slot contention, evaluate the suitability of the current pricing model (On-Demand vs. Editions), ensure critical pipelines have dedicated resources, and reduce operational noise.

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


## 1. On-Demand vs. Slot Pricing Comparison

**Objective:** Determine if queries should run on On-Demand or Autoscaling Slots (Editions).
**Methodology:** Compare theoretical On-Demand cost ($6.25/TiB) vs. Slot cost ($0.06/slot-hour).
**Success Criteria:** Identify queries where switching to Slots is >20% cheaper.


```python
query_pricing_comparison = f'''
SELECT
  project_id,  -- Added project_id
  job_id,
  creation_time,
  user_email,
  total_bytes_billed,
  total_slot_ms,
  -- On-Demand Cost: $6.25 per TiB (US Multi-region)
  (total_bytes_billed / POW(1024, 4)) * 6.25 AS estimated_on_demand_cost,
  -- Slot Cost: $0.06 per slot-hour (Approx. Enterprise Standard)
  (total_slot_ms / (1000 * 3600)) * 0.06 AS estimated_slot_cost,
  query
FROM
  `{PROJECT_ID}.bq_bestpractices_checklist.jobs_by_top_20_projects`
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
  AND total_bytes_billed > 0
ORDER BY
  estimated_on_demand_cost DESC
LIMIT 100
'''

df_pricing = client.query(query_pricing_comparison).to_dataframe()

# Apply logic: Switch to SLOT if slot cost is at least 20% cheaper than on-demand
df_pricing['recommendation'] = 'KEEP_ON_DEMAND'
df_pricing.loc[df_pricing['estimated_slot_cost'] < (df_pricing['estimated_on_demand_cost'] * 0.8), 'recommendation'] = 'SWITCH_TO_SLOTS'

print("Pricing Model Comparison (Top 10 High Cost Queries):")
df_pricing[['job_id', 'estimated_on_demand_cost', 'estimated_slot_cost', 'recommendation']].head(10)
```


```python
# Aggregated Analysis: Total Cost Comparison
total_on_demand_cost = df_pricing['estimated_on_demand_cost'].sum()
total_slot_cost = df_pricing['estimated_slot_cost'].sum()

print(f"Total Estimated On-Demand Cost: ${total_on_demand_cost:,.2f}")
print(f"Total Estimated Slot Cost:      ${total_slot_cost:,.2f}")

if total_slot_cost < (total_on_demand_cost * 0.8):
    savings = total_on_demand_cost - total_slot_cost
    print(f"\nRecommendation: SWITCH TO SLOTS. Potential savings: ${savings:,.2f} ({(savings/total_on_demand_cost)*100:.1f}%)")
elif total_slot_cost > total_on_demand_cost:
    extra_cost = total_slot_cost - total_on_demand_cost
    print(f"\nRecommendation: KEEP ON-DEMAND. Slots are estimated to be ${extra_cost:,.2f} more expensive.")
else:
    print("\nRecommendation: MIXED / NEUTRAL. Costs are comparable. Analyze specific high-cost queries for partial migration.")
```


**Recommendation:** For queries marked `SWITCH_TO_SLOTS`, consider moving them to a project/reservation using BigQuery Editions (Autoscaling) to save costs.


```python
print("\n--- Aggregated Analysis by Project ID ---\n")
df_grouped_by_project = df_pricing.groupby('project_id').agg(
    total_estimated_on_demand_cost=('estimated_on_demand_cost', 'sum'),
    total_estimated_slot_cost=('estimated_slot_cost', 'sum')
).reset_index()

def get_project_recommendation(row):
    on_demand_cost = row['total_estimated_on_demand_cost']
    slot_cost = row['total_estimated_slot_cost']

    if slot_cost < (on_demand_cost * 0.8):
        return 'SWITCH_TO_SLOTS'
    elif slot_cost > on_demand_cost:
        return 'KEEP_ON_DEMAND'
    else:
        return 'MIXED / NEUTRAL'

df_grouped_by_project['recommendation'] = df_grouped_by_project.apply(get_project_recommendation, axis=1)

print(df_grouped_by_project)
```


## 2. Slot Contention Analysis

**Objective:** Identify "Pending" states caused by slot starvation.
**Methodology:** Analyze `avg_pending_ms` for jobs.
**Success Criteria:** Minimize pending time for critical jobs.


```python
query_contention = f"""
SELECT
  project_id,
  job_type,
  COUNT(*) as total_jobs,
  AVG(TIMESTAMP_DIFF(start_time, creation_time, MILLISECOND)) as avg_pending_ms,
  MAX(TIMESTAMP_DIFF(start_time, creation_time, MILLISECOND)) as max_pending_ms
FROM
  `{PROJECT_ID}.bq_bestpractices_checklist.jobs_by_top_20_projects`
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND state = 'DONE'
GROUP BY 1, 2
HAVING avg_pending_ms > 1000
ORDER BY avg_pending_ms DESC
"""

df_contention = client.query(query_contention).to_dataframe()
print("Jobs with significant pending time (potential slot starvation):")
df_contention.head(10)
```


**Recommendation:** If `avg_pending_ms` is high, consider increasing max slots (if autoscaling) or baseline slots (if fixed).


## 3. Max Autoscaling Recommendation

**Objective:** Recommend a `max_autoscaling` value for reservations based on historical slot usage.
**Methodology:** Calculate the average slot usage per minute and then determine various percentiles (P95, P90, P85, P75, P50) of these per-minute averages.
**Success Criteria:** Provide data-driven recommendations for setting `max_autoscaling` to balance cost and performance.


```python
query_max_autoscaling = f'''
WITH per_minute_avg_slots AS (
  SELECT
    DATE_TRUNC(job_creation_time, MINUTE) AS minute_start,
    AVG(period_slot_ms) AS avg_slots_per_minute
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
  WHERE
    job_creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY
    1
)
SELECT
  APPROX_QUANTILES(avg_slots_per_minute, 100)[OFFSET(95)] / 1000 AS p95_slots_per_minute,
  APPROX_QUANTILES(avg_slots_per_minute, 100)[OFFSET(90)] / 1000 AS p90_slots_per_minute,
  APPROX_QUANTILES(avg_slots_per_minute, 100)[OFFSET(85)] / 1000 AS p85_slots_per_minute,
  APPROX_QUANTILES(avg_slots_per_minute, 100)[OFFSET(75)] / 1000 AS p75_slots_per_minute,
  APPROX_QUANTILES(avg_slots_per_minute, 100)[OFFSET(60)] / 1000 AS p60_slots_per_minute,
  APPROX_QUANTILES(avg_slots_per_minute, 100)[OFFSET(50)] / 1000 AS p50_slots_per_minute,
  MAX(avg_slots_per_minute) / 1000 AS max_slots_per_minute,
  AVG(avg_slots_per_minute) / 1000 AS avg_slots_per_minute_total
FROM per_minute_avg_slots
'''

df_max_autoscaling = client.query(query_max_autoscaling).to_dataframe()
print("Max Autoscaling Recommendation Metrics:")
df_max_autoscaling
```


**Recommendation:** The calculated percentiles of average slots per minute provide insights for setting `max_autoscaling` for reservations:
-   **P95:** A good starting point for `max_autoscaling` if you want to ensure high performance and minimize queueing for 95% of the time, accepting higher costs.
-   **P90:** A balance between performance and cost. Setting `max_autoscaling` to this value would cover 90% of your peak minute slot demands.
-   **P85:** Another balanced option, covering 85% of your peak minute slot demands.
-   **P75:** A more cost-effective option, covering 75% of your peak minute slot demands, potentially with more frequent queueing during higher peaks.
-   **P50:** The most cost-effective option, covering 50% of your peak minute slot demands, which will likely lead to significant queueing during peak periods.

Choose a percentile that aligns with your Service Level Objectives (SLOs) and cost optimization goals.


**Recommendation:** The calculated percentiles of average slots per minute provide insights for setting `max_autoscaling` for reservations:
-   **P95:** A good starting point for `max_autoscaling` if you want to ensure high performance and minimize queueing for 95% of the time, accepting higher costs.
-   **P90:** A balance between performance and cost. Setting `max_autoscaling` to this value would cover 90% of your peak minute slot demands.
-   **P85:** Another balanced option, covering 85% of your peak minute slot demands.
-   **P75:** A more cost-effective option, covering 75% of your peak minute slot demands, potentially with more frequent queueing during higher peaks.
-   **P50:** The most cost-effective option, covering 50% of your peak minute slot demands, which will likely lead to significant queueing during peak periods.

Choose a percentile that aligns with your Service Level Objectives (SLOs) and cost optimization goals.


**Recommendation:** Ensure production projects are assigned to specific reservations, not just the `default` pool.


## 4. Error Analysis

**Objective:** Reduce operational noise and retries.
**Methodology:** Summarize top error codes.


```python
query_errors = f"""
SELECT
  error_result.reason as error_reason,
  COUNT(*) as error_count
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE
  creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND error_result IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
"""

df_errors = client.query(query_errors).to_dataframe()
print("Top Error Codes:")
df_errors
```


**Recommendation:** Address top errors. For `rateLimitExceeded`, implement backoff. For `resourcesExceeded`, increase slots or optimize queries.


## 6. Google Cloud Recommendations

**Objective:** Review automated recommendations provided by Google Cloud.
**Methodology:** Query `INFORMATION_SCHEMA.RECOMMENDATIONS_BY_ORGANIZATION`.


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
WHERE
  recommender LIKE '%capacity%' OR recommender LIKE '%slot%'
ORDER BY
  slot_hours_saved_monthly DESC
LIMIT 20
"""

try:
    df_recommendations = client.query(query_recommendations).to_dataframe()
    print("Top Google Cloud Resource Recommendations:")
    df_recommendations.head()
except Exception as e:
    print("Error querying Recommendations.", e)
    df_recommendations = pd.DataFrame() # Empty DF for summary
```


**Recommendation:** Review capacity-related recommendations.


## 7. AI-Powered Recommendations

**Objective:** Use Generative AI to synthesize findings.
**Methodology:** Summarize findings and use Gemini Pro via BQML.


```python
%%bigquery
CREATE OR REPLACE MODEL `bq_bestpractices_checklist.gemini`
REMOTE WITH CONNECTION `us.llm`
OPTIONS (endpoint = 'https://aiplatform.googleapis.com/v1/projects/dataml-latam-argolis/locations/global/publishers/google/models/gemini-3-pro-preview')
```


```python
# Summarize findings for Gemini
summary_text = f'''
Audit Findings for Resource Management:
1. Pricing Model: Analyzed {len(df_pricing)} queries. Found {len(df_pricing[df_pricing['recommendation'] == 'SWITCH_TO_SLOTS'])} queries where switching to Slots is recommended.
2. Contention: Found {len(df_contention)} projects with significant pending time.
3. Slot Usage: Max slots per minute is {df_max_autoscaling['max_slots_per_minute'].iloc[0] if not df_max_autoscaling.empty else 'N/A'}, Average slots per minute is {df_max_autoscaling['avg_slots_per_minute_total'].iloc[0] if not df_max_autoscaling.empty else 'N/A'}.
4. Errors: Top error is {df_errors['error_reason'].iloc[0] if not df_errors.empty else 'None'}.
5. Google Cloud Recommendations: Found {len(df_recommendations)} active capacity recommendations.
'''

# Construct the prompt
prompt = f"Analyze the following metrics regarding BigQuery Resource Management and provide 3 actionable recommendations. Context: {summary_text}"

# Escape single quotes in prompt for BigQuery SQL literal
escaped_prompt = prompt.replace("'", "\'")

# Call Gemini via BQML
query_gemini = f"""
SELECT
  ml_generate_text_result['candidates'][0]['content'] AS recommendation
FROM
  ML.GENERATE_TEXT(
    MODEL `{PROJECT_ID}.bq_bestpractices_checklist.gemini`,
    (SELECT '''{escaped_prompt}''' AS prompt),
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
        print(row['recommendation'])
except Exception as e:
    print("Error calling Gemini. Ensure the model exists and you have permissions.", e)
```

