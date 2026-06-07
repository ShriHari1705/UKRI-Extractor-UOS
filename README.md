# UKRI Early Warning System

> **Give IT Services advance notice of compute-heavy grants — before researchers submit support tickets.**

This pipeline fetches funded projects from the [UKRI Gateway to Research API](https://gtr.ukri.org/gtr/api), filters for Innovate UK grants, and tags project abstracts by technology category (ML/AI, HPC, Data-Intensive, Cloud). IT teams can see what's coming before it hits the SLURM queue.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PRODUCTION STACK                             │
│                                                                     │
│  UKRI API          AWS S3              Snowflake                    │
│  (source)   ──►   (Data Lake)  ──►    RAW layer   ──►  dbt models   │
│                                           │                         │
│                  Apache Kafka         STAGING + MARTS               │
│                  (streaming alt)          │                         │
│                                     BI Dashboard                    │
│                  Apache Airflow      (Power BI)                     │
│                  (orchestration)                                    │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                     HPC / LOCAL (dev mode)                          │
│                                                                     │
│  UKRI API  ──►  cache/ukri_pages/  ──►  run_pipeline.py  ──►  CSV │
│                 (local JSON cache)                                  │
└─────────────────────────────────────────────────────────────────────┘
```

### Data flow

| Step | Tool | What happens |
|------|------|-------------|
| Extract | `fetcher.py` | Pages through all 173k UKRI projects, caches each page as JSON in S3 or local disk |
| Filter | `transformer.py` | Keeps only `leadFunder == "Innovate UK"` |
| Tag | `transformer.py` | Matches abstract text against keyword taxonomy — long format output |
| Load | `loader.py` | Writes CSV + incrementally flushes to Snowflake RAW every 1000 pages |
| Transform | dbt | Staging view deduplicates; mart builds compute_score + priority |
| Orchestrate | Airflow | Runs weekly, pre-warms Snowflake warehouse, triggers dbt after pipeline |
| Stream alt | Kafka | Producer publishes project events; consumer writes to Snowflake with DLQ |

---

## Repo structure

```
ukri-early-warning/
├── run_pipeline.py              # Entry point — run this
├── config.py                   # All settings (keywords, API, credentials)
├── docker-compose.yml          # Kafka + Zookeeper + Airflow + Postgres
│
├── pipeline/
│   ├── models.py               # Pydantic schemas (API v7 validation + HTML decode)
│   ├── fetcher.py              # Paginated API fetch + S3/local caching
│   ├── transformer.py          # Funder filter + keyword tagging + incremental Snowflake flush
│   └── loader.py               # CSV + Snowflake write (SQL injection safe)
│
├── airflow/
│   └── dags/
│       └── ukri_pipeline_dag.py   # Weekly Airflow DAG with Snowflake prewarm
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── seeds/
│   │   └── keyword_taxonomy.csv
│   └── models/
│       ├── staging/
│       │   └── stg_ukri_raw_projects.sql
│       └── marts/
│           ├── mart_keyword_tags.sql
│           └── mart_early_warning_signal.sql
│
├── kafka/
│   ├── producer.py             # Publishes project events to Kafka topic
│   └── consumer.py             # Reads from Kafka, writes to Snowflake (with DLQ)
│
├── tests/
│   └── test_pipeline.py        # Unit tests — run with pytest
│
├── slurm_job.sh                # HPC batch submission script (Stanage / Bessemer)
├── requirements.txt
└── .gitignore
```

---

## Quickstart — Local

### 1. Clone and install

```bash
git clone https://github.com/ShriHari1705/UKRI-Extractor-UOS.git
cd ukri-early-warning
pip install -r requirements.txt
```

### 2. Configure

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Minimum required for a local CSV run — no credentials needed. Add Snowflake + AWS credentials for production mode.

### 3. Dev run (first 5 pages)

```bash
python run_pipeline.py --pages 5
```

Output: `outputs/ukri_innovate_uk_tagged_YYYYMMDD_HHMM.csv`

### 4. Full run with Snowflake

```bash
python run_pipeline.py --snowflake
# ~3-4 hours on first run (8,720 pages)
# Flushes to Snowflake every 1000 pages — safe to interrupt and resume
```

### 5. Full backfill (truncate and reload)

```bash
python run_pipeline.py --snowflake --overwrite
```

### 6. Recent projects (upcoming + newly awarded)

```bash
python run_pipeline.py --recent --snowflake
# Fetches projects sorted by start date descending
# Stops when dates go below 2025-01-01 (configurable via --since)
python run_pipeline.py --recent --snowflake --since 2024-01-01
```

---

## Docker (Kafka + Airflow)

Requires Docker Desktop running.

```bash
docker-compose up -d
```

This starts:
- **Kafka** on `localhost:9092`
- **Zookeeper** (Kafka dependency)
- **Airflow webserver** on `http://localhost:8080` (login: `admin` / `admin`)
- **Airflow scheduler**
- **Postgres** (Airflow metadata DB)

```bash
docker-compose down   # stop all services
```

---

## Snowflake setup

Run once in a Snowflake worksheet as ACCOUNTADMIN:

```sql
CREATE ROLE UKRI_EWS_ROLE;
CREATE WAREHOUSE COMPUTE_WH WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
CREATE DATABASE UKRI_EWS;
CREATE SCHEMA UKRI_EWS.RAW;

CREATE USER ukri_pipeline
    PASSWORD = '<your-password>'
    DEFAULT_ROLE = UKRI_EWS_ROLE
    DEFAULT_WAREHOUSE = COMPUTE_WH
    DEFAULT_NAMESPACE = UKRI_EWS.RAW;

GRANT ROLE UKRI_EWS_ROLE TO USER ukri_pipeline;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE UKRI_EWS_ROLE;
GRANT ALL ON DATABASE UKRI_EWS TO ROLE UKRI_EWS_ROLE;
GRANT ALL ON SCHEMA UKRI_EWS.RAW TO ROLE UKRI_EWS_ROLE;
GRANT CREATE TABLE ON SCHEMA UKRI_EWS.RAW TO ROLE UKRI_EWS_ROLE;

-- Cost guard: hard stop at 5 credits, alert at 75%
CREATE RESOURCE MONITOR ukri_budget
    WITH CREDIT_QUOTA = 5
    TRIGGERS ON 75 PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = ukri_budget;
```

The pipeline creates `RAW.UKRI_PROJECTS` automatically on first run.

---

## AWS S3 setup

Create an IAM user with this inline policy (replace bucket name if different):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::ukri-early-warning",
        "arn:aws:s3:::ukri-early-warning/*"
      ]
    }
  ]
}
```

Add the access key and secret to `.env`:

```
USE_S3=true
S3_BUCKET=ukri-early-warning
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=eu-west-2
```

---

## dbt (transformation layer)

```bash
cd dbt
cp profiles.yml.example profiles.yml
# Edit profiles.yml with your Snowflake credentials

dbt seed       # load keyword taxonomy
dbt run        # build staging + mart models
dbt test       # run data quality tests
```

### Models

| Model | Schema | Description |
|-------|--------|-------------|
| `stg_ukri_raw_projects` | STAGING | Deduped, type-cast view of RAW |
| `mart_keyword_tags` | MARTS | Long format — one row per project × keyword |
| `mart_early_warning_signal` | MARTS | One row per project: compute_score, priority, starting_soon flag |

---

## HPC (Stanage / Bessemer — SLURM)

```bash
sbatch slurm_job.sh                            # full run
sbatch --export=ALL,MAX_PAGES=10 slurm_job.sh  # dev test
squeue -u $USER                                # monitor
tail -f logs/ukri_ews_<JOBID>.log
```

---

## Kafka (streaming mode)

```bash
# Terminal 1 — start consumer (writes to Snowflake, bad messages go to DLQ)
python kafka/consumer.py

# Terminal 2 — run producer
python kafka/producer.py --pages 10   # dev
python kafka/producer.py              # full run
```

Poison messages are automatically routed to `ukri.projects.dlq` with error metadata attached.

---

## Tests

```bash
pytest tests/
```

Covers: Pydantic parsing, funder filtering, keyword tagging, SQL identifier validation.

---

## Keyword taxonomy

Edit `KEYWORD_TAXONOMY` in `config.py`. Also update `dbt/seeds/keyword_taxonomy.csv` and run `dbt seed` to sync.

| Category | What it signals |
|----------|----------------|
| `ML_AI` | GPU demand, deep learning frameworks, LLM workloads |
| `HPC_Simulation` | SLURM jobs, MPI, CFD, molecular dynamics |
| `Data_Intensive` | Large storage needs, genomics pipelines, real-time ingestion |
| `Cloud_Infrastructure` | Researcher cloud onboarding, containerised workloads |

---

## Output columns

| Column | Description |
|--------|-------------|
| `project_id` | UKRI project UUID |
| `title` | Project title (HTML entities decoded) |
| `status` | Active / Closed |
| `lead_funder` | Always "Innovate UK" in this pipeline |
| `grant_category` | e.g. Collaborative R&D, Feasibility Studies |
| `start_date` | Grant start date |
| `end_date` | Grant end date |
| `category` | Keyword category (ML_AI, HPC_Simulation, …) |
| `keyword` | Specific keyword matched |
| `found_in` | Which text field(s) contained the keyword |
| `gtr_url` | Link to project on gtr.ukri.org |
| `ingested_at` | Pipeline run timestamp (UTC) |

---

## FAQ

**Q: Why does the pipeline fetch all 173k UKRI projects?**  
A: UKRI API v7 has no server-side funder filter. We fetch everything and filter on `leadFunder` in Python. The S3 cache means you only pay this cost once.

**Q: Is it safe to interrupt a full run?**  
A: Yes. S3 caches every fetched page. Snowflake receives incremental flushes every 1000 pages. Re-running resumes from the last uncached page automatically.

**Q: Some projects have no start date. Why?**  
A: A known data quality gap in the UKRI API — some records have null timestamps. These appear as `NULL` in Snowflake and are handled with `NULLS LAST` in the mart ORDER BY.

**Q: How do I add a new keyword?**  
A: Add it to `KEYWORD_TAXONOMY` in `config.py` and to `dbt/seeds/keyword_taxonomy.csv`, then run `dbt seed && dbt run`.

---

---

## Generative AI Usage Statement

In accordance with the University of Sheffield's policy on the use of generative AI tools, the following disclosure is made:

Generative AI coding assistants were used during the development of this project to support the following activities:

- **Code scaffolding**: generating boilerplate for Pydantic models, Snowflake connector setup, and dbt model structure
- **Debugging**: identifying root causes of errors (e.g. Snowflake date casting, S3 region configuration, dbt profile parsing)
- **Code review**: checking for security issues such as SQL injection vulnerabilities and suggesting fixes
- **Documentation**: drafting inline comments and sections of this README

All AI-generated code was reviewed, tested, and validated by the author before being committed. The system architecture, design decisions, keyword taxonomy, and analytical outputs are the author's own work. The author takes full responsibility for the correctness and integrity of the codebase.

---

*Built for the University of Sheffield IT Services team.*
