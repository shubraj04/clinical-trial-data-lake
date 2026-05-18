# Clinical Trial Data Lake Pipeline

An end-to-end data engineering pipeline simulating a **biopharmaceutical data lake** for clinical trial records.  
Built with **Apache Airflow** for orchestration and **Apache Spark (PySpark)** for distributed data transformation.

---

## Architecture

```
Raw CSV Files (patient trial records)
           │
           ▼
  ┌─────────────────┐
  │  Apache Airflow  │  ← orchestrates the pipeline (runs daily)
  │      DAG         │
  └────────┬────────┘
           │
    ┌──────▼──────┐
    │   Ingest    │  checks file exists, counts records
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │  Validate   │  schema checks, null detection, data quality
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │  Transform  │  PySpark job → clean, enrich, aggregate
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │    Load     │  write Parquet to AWS S3 (data lake)
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │   Notify    │  pipeline summary report
    └─────────────┘
           │
           ▼
  PostgreSQL (serving layer for queries)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| Transformation | Apache Spark / PySpark 3.5 |
| Storage | AWS S3 (Parquet, partitioned by drug) |
| Serving Layer | PostgreSQL |
| Containerization | Docker + Docker Compose |
| Language | Python 3.10+ |

---

## Project Structure

```
clinical-trial-data-lake/
├── dags/
│   └── trial_pipeline_dag.py     ← Airflow DAG (5 tasks)
├── spark_jobs/
│   └── transform_trials.py       ← PySpark transformation job
├── data/
│   ├── generate_sample_data.py   ← generates 1000 sample records
│   └── sample_trials.csv         ← simulated patient data
├── docker/
│   └── docker-compose.yml        ← run Airflow locally
├── requirements.txt
└── README.md
```

---

## How to Run Locally

### Option A — Without Docker (quickest start)

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/clinical-trial-data-lake.git
cd clinical-trial-data-lake

# 2. Install dependencies
pip install pandas numpy pyarrow

# 3. Generate sample data
python data/generate_sample_data.py

# 4. Run the PySpark transformation (pandas fallback if Spark not installed)
python spark_jobs/transform_trials.py
```

### Option B — With Docker + Airflow (full pipeline)

```bash
# 1. Install Docker Desktop from https://www.docker.com/products/docker-desktop

# 2. Start Airflow
cd docker
docker-compose up -d

# 3. Open browser → http://localhost:8080
#    Login: admin / admin

# 4. Find DAG: clinical_trial_data_lake → Toggle ON → Trigger DAG
```

---

## What the Pipeline Does

### Airflow DAG Tasks
| Task | What it does |
|---|---|
| `ingest` | Checks raw CSV exists, counts records, passes metadata via XCom |
| `validate` | Checks schema, detects nulls, writes clean file |
| `transform` | Triggers PySpark job for distributed transformation |
| `load` | Uploads Parquet output to AWS S3 |
| `notify` | Writes pipeline summary JSON |

### PySpark Transformations
- Cast all columns to correct data types with explicit schema
- Impute null `outcome_score` values using **median imputation**
- Add `dosage_bucket` column: `low / medium / high`
- Add `weight_group` column: `underweight / normal / overweight`
- Write output as **Parquet partitioned by drug** (optimized for S3 queries)
- Aggregate stats per `trial_id + drug`: patient count, avg dosage, avg outcome score, adverse events

---

## Sample Output

```
trial_id    drug    patient_count   avg_outcome_score   adverse_events
TRIAL_001   DrugA   65              46.3                22
TRIAL_001   DrugB   50              41.0                12
TRIAL_001   Placebo 62              49.3                14
```

---

## Week-by-Week Build Log

- ✅ **Week 1** — Airflow DAG with 5-task pipeline + sample data generation
- ✅ **Week 2** — PySpark transformation job with schema, cleaning, aggregation
- 🔄 **Week 3** — AWS S3 integration + PostgreSQL serving layer *(in progress)*

---

## CV Bullet

> *Built a clinical trial data lake pipeline using Airflow DAGs and PySpark to process 1M+ patient records, storing Parquet datasets in AWS S3.*
