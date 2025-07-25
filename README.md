# COVID-19 ETL Pipeline using Apache Airflow and Azure Data Lake

This project demonstrates a real-world ETL pipeline for COVID-19 data using **Apache Airflow**, with data stored in **Azure Data Lake** and optionally loaded into **Azure Synapse Analytics** for advanced analytics.

---

## Project Overview

- **Objective**: Build an automated data pipeline to:
  - Extract COVID-19 data from a public source or CSV.
  - Transform the data using Python.
  - Load the clean data into **Azure Data Lake Storage (ADLS Gen2)**.
  - Optionally, load the data into **Azure Synapse (Data Warehouse)**.

- **Tools & Technologies**:
  - Python
  - Apache Airflow (via Astro CLI)
  - Azure Data Lake Gen2
  - Azure Synapse (optional)
  - Pandas
  - Azure SDK for Python

---

## Pipeline Architecture

```plaintext
+-------------+        +-----------------+        +-------------------+        +---------------------+
|  Raw CSV or |  ==>   |  Airflow DAG    |  ==>   | Azure Data Lake   | ==>    | Azure Synapse (DW)  |
|  API Source |        | (ETL Script)    |        | (raw/covid.csv)   |        |  (Optional Load)    |
+-------------+        +-----------------+        +-------------------+        +---------------------+
