# BigQuery Best Practices Checklist

This repository contains a comprehensive checklist and a set of Jupyter notebooks designed to help you audit, optimize, and maintain your Google Cloud BigQuery environment. The checklist covers critical areas such as ingestion, storage, processing, resource management, data governance, and FinOps.

## Modules

The project is organized into the following modules, each with a dedicated notebook:

### 1. [Ingestion & Storage](01_ingestion_storage/01_ingestion_storage.ipynb)
*   **Objective:** Optimize data ingestion and storage costs.
*   **Key Checks:** Storage billing models (Logical vs. Physical), data layout (Partitioning/Clustering), and streaming efficiency.

### 2. [Processing & Performance](02_processing/02_processing.ipynb)
*   **Objective:** Analyze and improve query efficiency.
*   **Key Checks:** Pruning efficiency (Scan Efficiency Ratio), slot usage, and identification of candidates for Materialized Views or BI Engine.

### 3. [Resource Management](03_resource_management/03_resource_management.ipynb)
*   **Objective:** Manage slot capacity and reduce contention.
*   **Key Checks:** On-Demand vs. Editions pricing comparison, slot contention analysis, and workload management.

### 4. [Data Governance & Security](04_data_governance/04_data_governance.ipynb)
*   **Objective:** Ensure data security and compliance.
*   **Key Checks:** Open access risks (allUsers), Row-Level Security (RLS) policies, CMEK usage, and data quality (NULL keys).

### 5. [FinOps & Cost Allocation](05_finops/05_finops.ipynb)
*   **Objective:** Track and allocate costs effectively.
*   **Key Checks:** Billing export readiness, label coverage analysis, chargeback modeling, and commitment gap analysis.

## Prerequisites

Before running the notebooks, ensure you have the following:

*   **Google Cloud Project:** Access to a GCP project with BigQuery enabled.
*   **Permissions:** Sufficient permissions to view `INFORMATION_SCHEMA` views (e.g., BigQuery Resource Admin, BigQuery Data Viewer).
*   **APIs Enabled:**
    *   BigQuery API
    *   Vertex AI API (required for Gemini-powered recommendations)
*   **Python Environment:** Python 3.8+

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd bq-bestpractices-checklist
    ```

2.  **Install dependencies:**
    This project uses `pipenv` for dependency management.
    ```bash
    pipenv install
    pipenv shell
    ```
    Alternatively, you can install the requirements using pip:
    ```bash
    pip install pandas google-cloud-bigquery bigframes matplotlib seaborn ipykernel
    ```

3.  **Authenticate:**
    The notebooks use Application Default Credentials (ADC). Authenticate using `gcloud`:
    ```bash
    gcloud auth application-default login
    ```

## Usage

1.  Open the desired notebook (e.g., `jupyter notebook 01_ingestion_storage/01_ingestion_storage.ipynb`).
2.  Run the **Pre-requirements** cells at the top of the notebook to initialize the BigQuery client, create the Gemini model, and set up the organization-level view.
3.  Follow the instructions in the notebook to perform the audit and view recommendations.

## Contributors

*   Google Cloud Data Analytics Team

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.
