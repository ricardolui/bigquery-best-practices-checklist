# BigQuery Best Practices Assessment

This document outlines the current checks implemented in the assessment notebooks and provides enriched best practices based on internal Google guidelines (Duckie/Moma).

## 01. Ingestion & Storage

### Current Checks in Codebase
* Storage Model Optimization (Logical vs Physical cost comparison)
* Data Layout: Detecting Date-Sharded Tables
* Streaming Optimization (Legacy inserts detection)
* Time Travel & Fail-Safe Storage Cost Analysis
* Unused / Stale Table Identification (90+ days unmodified)
* AI-Powered Recommendations (Gemini)

### Enriched Best Practices
* **Data Ingestion Methods:** Use `LOAD DATA` for bulk loads (free pool), **Storage Write API** for high-throughput streaming (exactly-once semantics), and **Datastream** for CDC from operational databases. Avoid frequent single-row inserts/updates.
* **Partitioning & Clustering:** Always partition large tables (by Date/Timestamp/Integer) and enforce partition filters. Cluster high-cardinality columns frequently used in `WHERE` or `JOIN` clauses.
* **Storage Billing Models:** Evaluate Logical vs. Physical (Compressed) storage billing at the dataset level. Physical storage can be more cost-effective for highly compressible data. Use `INFORMATION_SCHEMA.TABLE_STORAGE` to analyze.
* **Lifecycle Management:** Implement partition expiration and table TTLs to automatically manage costs.

## 02. Processing & Performance

### Current Checks in Codebase
* Pruning Efficiency (Scan Efficiency Ratio)
* Acceleration Candidates (Materialized View candidates)
* Spill-to-Disk Events
* Anti-Patterns (via BigQuery Anti-Pattern Recognition Tool UDF)
* Google Cloud Recommendations
* Query Execution Plan Analysis (slot duration ratio)
* Vector Search / BQML Efficiency
* AI-Powered Recommendations (Gemini)

### Enriched Best Practices
* **Maximize Pruning:** Use constant expressions in partition filters. Isolate partition columns in comparisons.
* **Minimize Spill-to-Disk:** Filter data early before `JOIN`s or `GROUP BY`s to reduce shuffle volume. Optimize JOINs by avoiding highly skewed keys or keys with many NULLs.
* **Avoid Anti-Patterns:** Avoid `SELECT *`. Avoid point-specific DMLs (single row updates); batch them instead. Replace self-joins with Window Functions (`LAG`, `LEAD`).
* **Advanced Features:** Leverage Materialized Views for frequent/complex queries and rely on BigQuery Advanced Runtime for automated vectorization enhancements.

## 03. Resource Management

### Current Checks in Codebase
* On-Demand vs. Slot Pricing Comparison
* Slot Contention Analysis
* Max Autoscaling Recommendation (percentile-based)
* Error Analysis (top error codes)
* Google Cloud Recommendations (capacity-related)
* Transition to BigQuery Editions
* Workload Management / Concurrency (queue time analysis)
* AI-Powered Recommendations (Gemini)

### Enriched Best Practices
* **Editions & Autoscaling:** Utilize BigQuery Editions (Standard, Enterprise, Enterprise Plus) based on workload criticality. Leverage autoscaling to dynamically adjust compute capacity and only pay for used slots.
* **Workload Isolation with Reservations:** Assign reservations to specific projects/folders. Rely on idle slot sharing to maximize resource utilization across the organization.
* **Right-Sizing:** Monitor slot utilization, job concurrency, and queue times using `INFORMATION_SCHEMA.JOBS` to adjust maximum slot limits and avoid contention.
* **Project Architecture:** Separate resources into distinct projects: Administration (for reservations), Data (for storage), and Compute (for query execution).

## 04. Data Governance & Security

### Current Checks in Codebase
* Audit Open Access Risks (IAM policy verification)
* Data Catalog Verification (Dataplex glossaries, aspect types, entry types)
* Data Profiling (SQL-based table profiling)
* Row-Level (RLS) and Column-Level Security (CLS) Coverage
* Authorized Views & Routines Validation
* Verify Encryption (CMEK)
* Data Quality Check (NULL Keys)

### Enriched Best Practices
* **Granular IAM:** Apply the Principle of Least Privilege. Prefer Dataset-level roles for team access over Project-level roles. Use Google Groups for easier management at scale.
* **Column-Level Security & Masking:** Use Policy Tags (via Dataplex) to classify sensitive data (PII) and restrict column access. Implement Data Masking (e.g., SHA256, Last Four Characters) to obscure sensitive data for users without full access.
* **Row-Level Security:** Implement row-level access policies (`FILTER USING`) to restrict data visibility based on user context (e.g., business unit, region).
* **Additional Controls:** Utilize Authorized Views to share results without underlying table access. Implement VPC Service Controls (VPC-SC) to prevent data exfiltration.

## 05. FinOps & Cost Allocation

### Current Checks in Codebase
* Billing Export Readiness
* Label Coverage Analysis
* Chargeback Modeling (by user, $6.25/TiB On-Demand)
* Commitment Gap Analysis (usage vs committed slots)
* Tags vs. Labels (Resource Manager Tags for chargeback)
* Idle Project / Dataset Cost Analysis
* AI-Powered Recommendations (Gemini)

### Enriched Best Practices
* **Standardized Labeling:** Implement a strict labeling taxonomy (e.g., `team`, `environment`, `cost-center`) on datasets, tables, jobs, and reservations. Automate via IaC (Terraform).
* **Billing Export Analysis:** Enable detailed BigQuery Billing Export. Use labels in the export data to perform showback/chargeback to specific teams.
* **Commitment Optimization:** Analyze slot usage to determine the optimal mix of on-demand, flex slots, and long-term commitments (1-year or 3-year) for Enterprise/Enterprise Plus editions. Use the Slot Recommender.
