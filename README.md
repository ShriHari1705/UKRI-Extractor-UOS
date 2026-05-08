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
│  (source)   ──►   (Data Lake)  ──►    RAW layer   ──►  dbt models  │
│                                           │                         │
│                  Apache Kafka         STAGING + MARTS               │
│                  (streaming alt)          │                         │
│                                     BI Dashboard                    │
│                  Apache Airflow      (Tableau / Metabase)           │
│                  (orchestration)                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                     HPC / LOCAL (dev mode)                          │
│                                                                     │
│  UKRI API  ──►  cache/ukri_pages/  ──►  run_pipeline.py  ──►  CSV │
│                 (local JSON cache)                                  │
└─────────────────────────────────────────────────────────────────────┘
```

### Data flow detail

| Step | Tool | What happens |
|------|------|-------------|
| Extract | `fetcher.py` | Pages through all 173k UKRI projects, caches each page as JSON |
| Filter | `transformer.py` | Keeps only `leadFunder == "Innovate UK"` |
| Tag | `transformer.py` | Matches abstract text against keyword taxonomy — long format output |
| Load | `loader.py` | Writes CSV + optionally loads to Snowflake RAW layer |
| Transform | dbt | Staging view deduplicates; mart builds compute_score + priority |
| Orchestrate | Airflow | Runs weekly, triggers dbt after pipeline completes |
| Stream alt | Kafka | Producer publishes project events; consumer writes to Snowflake |

---

## Repo structure

```
ukri-early-warning/
├── run_pipeline.py              # Entry point — run this
├── config.py                   # All settings (keywords, API, credentials)
│
├── pipeline/
│   ├── models.py               # Pydantic schemas (API v7 validation)
│   ├── fetcher.py              # Paginated API fetch + S3/local caching
│   ├── transformer.py          # Funder filter + keyword tagging (long format)
│   └── loader.py               # CSV + Snowflake write
│
├── airflow/
│   └── dags/
│       └── ukri_pipeline_dag.py   # Weekly Airflow DAG
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── seeds/
│   │   └── keyword_taxonomy.csv   # keyword list as dbt seed
│   └── models/
│       ├── staging/
│       │   └── stg_ukri_raw_projects.sql
│       └── marts/
│           ├── mart_keyword_tags.sql          # long format (project × keyword)
│           └── mart_early_warning_signal.sql  # one row per project, priority scored
│
├── kafka/
│   ├── producer.py             # Publishes project events to Kafka topic
│   └── consumer.py             # Reads from Kafka, writes to Snowflake
│
├── slurm_job.sh                # HPC batch submission script (Stanage / Bessemer)
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Quickstart — Local / HPC

### 1. Clone and set up

```bash
git clone https://github.com/your-org/ukri-early-warning.git
cd ukri-early-warning

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env — at minimum you need nothing for a local CSV run.
# Add Snowflake credentials only if you want --snowflake output.
```

### 3. Run (dev mode — first 5 pages)

```bash
python run_pipeline.py --pages 5
```

Output: `outputs/ukri_innovate_uk_tagged_YYYYMMDD_HHMM.csv`

### 4. Full run

```bash
python run_pipeline.py
# ~3-4 hours on first run (8,660 pages); subsequent runs use local cache
```

### 5. Full run with Snowflake load

```bash
python run_pipeline.py --snowflake
```

---

## HPC (Stanage / Bessemer — SLURM)

```bash
# Full run
sbatch slurm_job.sh

# Dev test — first 10 pages
sbatch --export=ALL,MAX_PAGES=10 slurm_job.sh

# Monitor
squeue -u $USER
tail -f logs/ukri_ews_<JOBID>.log
```

The SLURM script loads the Python module, activates `.venv`, and runs `run_pipeline.py --snowflake`. Edit `slurm_job.sh` to adjust partition name, memory, and email.

---

## Snowflake setup

Run once to create the database and warehouse:

```sql
CREATE DATABASE IF NOT EXISTS UKRI_EWS;
CREATE SCHEMA  IF NOT EXISTS UKRI_EWS.RAW;
CREATE SCHEMA  IF NOT EXISTS UKRI_EWS.STAGING;
CREATE SCHEMA  IF NOT EXISTS UKRI_EWS.MARTS;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60;
```

The pipeline creates the `RAW.UKRI_PROJECTS` table automatically on first run.

---

## dbt (transformation layer)

```bash
cd dbt
cp profiles.yml.example profiles.yml
# Edit profiles.yml with your Snowflake credentials

# Load keyword seed table
dbt seed

# Run all models
dbt run

# Test
dbt test
```

### Key models

| Model | Schema | Description |
|-------|--------|-------------|
| `stg_ukri_raw_projects` | STAGING | Deduped, Innovate UK filtered view of RAW |
| `mart_keyword_tags` | MARTS | Long format — one row per project × keyword |
| `mart_early_warning_signal` | MARTS | One row per project: compute_score, priority, starting_soon flag |

---

## Airflow (orchestration)

```bash
# Copy DAG to your Airflow dags folder
cp airflow/dags/ukri_pipeline_dag.py $AIRFLOW_HOME/dags/

# Set the pipeline directory variable in Airflow UI:
# Admin → Variables → UKRI_PIPELINE_DIR = /path/to/ukri-early-warning
```

DAG runs every Monday at 06:00 UTC: fetch → dbt staging → dbt marts → dbt tests.

---

## Kafka (streaming mode)

Requires a running Kafka broker. Set `KAFKA_BOOTSTRAP` in `.env`.

```bash
# Terminal 1 — start consumer (writes to Snowflake as events arrive)
python kafka/consumer.py

# Terminal 2 — run producer (publishes Innovate UK projects to topic)
python kafka/producer.py --pages 10   # dev
python kafka/producer.py              # full run
```

---

## Keyword taxonomy

Edit `KEYWORD_TAXONOMY` in `config.py` to add or adjust keywords. The same taxonomy is reflected in `dbt/seeds/keyword_taxonomy.csv` — update both, then run `dbt seed` to sync.

Categories:

| Category | What it signals |
|----------|----------------|
| `ML_AI` | GPU demand, deep learning frameworks, LLM workloads |
| `HPC_Simulation` | SLURM jobs, MPI, CFD, molecular dynamics |
| `Data_Intensive` | Large storage needs, genomics pipelines, real-time ingestion |
| `Cloud_Infrastructure` | Researcher cloud onboarding, containerised workloads |

---

## Output columns (long format)

| Column | Description |
|--------|-------------|
| `project_id` | UKRI project UUID |
| `title` | Project title |
| `status` | Active / Closed |
| `lead_funder` | Always "Innovate UK" in this pipeline |
| `grant_category` | Research Council, Innovate UK, etc. |
| `start_date` | Grant start date (from Unix ms timestamp) |
| `end_date` | Grant end date |
| `category` | Keyword category (ML_AI, HPC_Simulation, …) |
| `keyword` | Specific keyword matched |
| `found_in` | Which text field(s) contained the keyword |
| `gtr_url` | Link to project on gtr.ukri.org |
| `ingested_at` | Pipeline run timestamp |

---

## Why long format?

Wide format (one boolean column per keyword) makes simple queries impossible without string parsing:

```sql
-- Wide format: how many ML projects? Requires CASE WHEN on a boolean column.
-- Long format: trivial GROUP BY
SELECT category, COUNT(DISTINCT project_id) AS projects
FROM mart_keyword_tags
GROUP BY category;
```

---

## FAQ

**Q: Why does the pipeline fetch all 173k UKRI projects instead of filtering by funder?**  
A: UKRI API v7 has no server-side funder filter endpoint. We fetch everything and filter on `leadFunder` in Python. The cache means you only pay this cost once.

**Q: First run takes hours — is that normal?**  
A: Yes — 8,660 pages × 0.25s delay = ~36 minutes minimum, plus API latency. Subsequent runs skip cached pages and complete in seconds. Use `--pages 10` during development.

**Q: Some projects have no start date. Why?**  
A: A known data quality gap in the UKRI API — some records have null `start`/`end` timestamps. These appear as `NaT` in the DataFrame and `NULL` in Snowflake. The `mart_early_warning_signal` model handles these gracefully (`NULLS LAST` in ORDER BY).

**Q: How do I add a new keyword?**  
A: Add it to `KEYWORD_TAXONOMY` in `config.py` and to `dbt/seeds/keyword_taxonomy.csv`, then run `dbt seed && dbt run`.

---

*Built for the University of Sheffield IT Services team. Contacts: Shri Hari (pipeline), Joe Heffer (Research IT).*
