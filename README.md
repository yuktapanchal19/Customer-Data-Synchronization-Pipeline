# Customer Data Synchronization Pipeline

An automated data engineering project using **Apache Airflow, Python, Pandas, and Snowflake** to ingest, transform, validate, and report customer and sales data.

## 🚀 Overview

The project contains two daily Airflow pipelines:

* **Customer Pipeline (`customer_sync`)** — Loads customer data, enriches customer records, performs data-quality checks, and generates a summary.
* **Sales Pipeline (`daily_sales_loader`)** — Loads sales data, creates regional sales summaries, performs validation checks, and generates a sales report.

### Architecture

```text
CSV Files
    │
    ▼
Apache Airflow
    │
    ├── Customer Pipeline ──► Snowflake
    │                          ├── RAW_CUSTOMERS
    │                          └── ENRICHED_CUSTOMERS
    │
    └── Sales Pipeline ─────► Snowflake
                               ├── RAW_SALES
                               └── SALES_SUMMARY
```

## 🛠️ Tech Stack

* Python
* Apache Airflow
* Astronomer Runtime
* Snowflake
* Pandas
* SQL
* Docker
* Git & GitHub

## 📁 Project Structure

```text
├── dags/
│   ├── customer_pipeline.py
│   └── sales_pipeline.py
├── data/
│   ├── customers_data.csv
│   └── sales_data.csv
├── tests/
├── Dockerfile
├── requirements.txt
├── packages.txt
└── README.md
```

## ▶️ Run Locally

Clone the repository:

```bash
git clone https://github.com/yuktapanchal19/Customer-Data-Synchronization-Pipeline.git
cd Customer-Data-Synchronization-Pipeline
```

Start Airflow:

```bash
astro dev start
```

Open the Airflow UI:

```text
http://localhost:8080
```

Configure the Airflow connection:

```text
Connection ID: snowflake_default
```

Then trigger either DAG from the Airflow UI.

## ✅ Key Features

* Automated daily ETL pipelines
* CSV to Snowflake ingestion
* Customer data enrichment
* Regional sales aggregation
* Data-quality validation
* Airflow retries and failure handling
* Docker-based local development

## 👩‍💻 Author

**Yukta Panchal**

Data Engineering | Python | SQL | Airflow | Snowflake
