# UKRI Early Warning System

> **Give IT Services advance notice of compute-heavy grants — and give Research & Policy Intelligence teams a live view of the UKRI funding landscape.**

This system has two audiences served by the same pipeline and dashboard:

- **IT Services (Early Warning tab)** — fetches Innovate UK grants, tags abstracts by technology category (ML/AI, HPC, Data-Intensive, Cloud), and scores projects by compute demand. IT teams see what's coming before it hits the SLURM queue.
- **RPI Growth Managers (RPI tab)** — loads all UKRI funders (EPSRC, MRC, BBSRC, AHRC, ESRC, NERC, STFC, Innovate UK, etc.) into a funding landscape view, showing trends by funder, grant category, and year to support bid strategy.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PRODUCTION STACK                             │
│                                                                     │
│  UKRI API          AWS S3              MotherDuck                   │
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

**IT Services pipeline (`run_pipeline.py`)**

| Step | Tool | What happens |
|------|------|-------------|
| Extract | `fetcher.py` | Pages through all ~175k UKRI projects, caches each page as JSON in S3 or local disk |
| Filter | `transformer.py` | Keeps only `leadFunder == "Innovate UK"` |
| Tag | `transformer.py` | Matches abstract text against keyword taxonomy — long format output |
| Load | `loader.py` | Writes CSV + incrementally flushes to MotherDuck `RAW.UKRI_PROJECTS` every 1000 pages |
| Transform | dbt | Staging view deduplicates; mart builds compute_score + priority tier |
| Orchestrate | Airflow | Runs weekly, triggers dbt after pipeline |
| Stream alt | Kafka | Producer publishes project events; consumer writes to MotherDuck with DLQ |

**RPI pipeline (`run_rpi_pipeline.py`)**

| Step | Tool | What happens |
|------|------|-------------|
| Extract | `fetcher.py` | Reuses the same local page cache — no re-fetching |
| Parse | `rpi_transformer.py` | Extracts metadata for **all funders**, no keyword tagging — one row per project |
| Load | `loader.py` | Incrementally flushes to MotherDuck `RAW.UKRI_ALL_PROJECTS` every 500 pages |
| Transform | dbt | Staging view deduplicates; mart adds activity flags (currently active, started last 2 years) |

---

## Repo structure

```
ukri-early-warning/
├── run_pipeline.py              # IT Services pipeline — Innovate UK, keyword-tagged
├── run_rpi_pipeline.py          # RPI pipeline — all funders, wide-format metadata
├── config.py                   # All settings (keywords, API, MotherDuck table names)
├── dashboard.py                 # Streamlit dashboard (two tabs: EWS + RPI)
├── docker-compose.yml          # Kafka + Zookeeper + Airflow + Postgres
│
├── pipeline/
│   ├── models.py               # Pydantic schemas (API v7 validation + HTML decode)
│   ├── fetcher.py              # Paginated API fetch + S3/local caching
│   ├── transformer.py          # Innovate UK filter + keyword tagging (IT Services)
│   ├── rpi_transformer.py      # All-funder extractor, no keyword tagging (RPI)
│   └── loader.py               # CSV + MotherDuck write for both pipelines
│
├── airflow/
│   └── dags/
│       └── ukri_pipeline_dag.py   # Weekly Airflow DAG
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── seeds/
│   │   └── keyword_taxonomy.csv
│   └── models/
│       ├── staging/
│       │   ├── stg_ukri_raw_projects.sql    # Innovate UK (IT Services)
│       │   └── stg_ukri_all_projects.sql    # All funders (RPI)
│       └── marts/
│           ├── mart_keyword_tags.sql
│           ├── mart_early_warning_signal.sql
│           └── mart_rpi_funding_landscape.sql
│
├── kafka/
│   ├── producer.py             # Publishes project events to Kafka topic
│   └── consumer.py             # Reads from Kafka, writes to MotherDuck (with DLQ)
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

Minimum required for a local CSV run — no credentials needed. Add MotherDuck + AWS credentials for production mode.

### 3. Dev run (first 5 pages)

```bash
python run_pipeline.py --pages 5
```

Output: `outputs/ukri_innovate_uk_tagged_YYYYMMDD_HHMM.csv`

### 4. Full run with MotherDuck (IT Services pipeline)

```bash
python run_pipeline.py --motherduck
# ~3-4 hours on first run (8,720 pages)
# Flushes to MotherDuck every 1000 pages — safe to interrupt and resume
```

### 5. Full backfill (truncate and reload)

```bash
python run_pipeline.py --motherduck --overwrite
```

### 6. Recent projects (upcoming + newly awarded)

```bash
python run_pipeline.py --recent --motherduck
# Fetches projects sorted by start date descending
# Stops when dates go below 2025-01-01 (configurable via --since)
python run_pipeline.py --recent --motherduck --since 2024-01-01
```

### 7. RPI Growth Manager pipeline (all funders)

Run this after step 4 — it reuses the local page cache so no re-fetching is needed:

```bash
python run_rpi_pipeline.py --motherduck --overwrite
# Loads ~175k projects across all UKRI councils into UKRI_ALL_PROJECTS
```

Then build the dbt models:

```bash
dbt run --project-dir dbt --select stg_ukri_all_projects mart_rpi_funding_landscape
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

## MotherDuck setup

1. Create a free account at [motherduck.com](https://motherduck.com) and generate a service token.
2. Add it to `.env`:

```
MOTHERDUCK_TOKEN=<your-token>
MOTHERDUCK_DATABASE=UKRI_EWS
MOTHERDUCK_SCHEMA=RAW
```

There's no warehouse or role setup required — MotherDuck is serverless and databases/schemas are created on demand. The pipeline creates the `UKRI_EWS` database, `RAW` schema, and `RAW.UKRI_PROJECTS` table automatically on first run.

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
# profiles.yml reads MOTHERDUCK_TOKEN from the environment — no editing needed
# if .env is already configured

dbt seed       # load keyword taxonomy
dbt run        # build staging + mart models
dbt test       # run data quality tests
```

### Models

| Model | Schema | Audience | Description |
|-------|--------|----------|-------------|
| `stg_ukri_raw_projects` | STAGING | IT Services | Deduped Innovate UK view of RAW.UKRI_PROJECTS |
| `stg_ukri_all_projects` | STAGING | RPI | Deduped all-funder view of RAW.UKRI_ALL_PROJECTS |
| `mart_keyword_tags` | MARTS | IT Services | Long format — one row per project × keyword |
| `mart_early_warning_signal` | MARTS | IT Services | One row per project: compute_score, priority, starting_soon |
| `mart_rpi_funding_landscape` | MARTS | RPI | One row per project across all funders, with activity flags |

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
# Terminal 1 — start consumer (writes to MotherDuck, bad messages go to DLQ)
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

**Q: Why does the pipeline fetch all ~175k UKRI projects?**  
A: UKRI API v7 has no server-side funder filter. We fetch everything and filter on `leadFunder` in Python. The S3/local cache means you only pay this cost once — the RPI pipeline reuses the same cache.

**Q: Is it safe to interrupt a full run?**  
A: Yes. S3/local cache saves every fetched page. MotherDuck receives incremental flushes periodically. Re-running resumes from the last uncached page automatically.

**Q: Some projects have no start date. Why?**  
A: A known data quality gap in the UKRI API — some records have null or invalid timestamps. These appear as `NULL` in MotherDuck and are handled gracefully in both pipelines.

**Q: How do I add a new keyword?**  
A: Add it to `KEYWORD_TAXONOMY` in `config.py` and to `dbt/seeds/keyword_taxonomy.csv`, then run `dbt seed && dbt run`.

**Q: What's the difference between the two pipelines?**  
A: `run_pipeline.py` targets IT Services — it filters to Innovate UK, tags abstracts by compute keyword, and scores projects by infrastructure demand. `run_rpi_pipeline.py` targets RPI Growth Managers — it loads all UKRI funders with no keyword tagging, providing a broad funding landscape view for bid strategy.

---

---

## Generative AI Usage Statement

In accordance with the University of Sheffield's policy on the use of generative AI tools, the following disclosure is made:

Generative AI coding assistants were used during the development of this project to support the following activities:

- **Code scaffolding**: generating boilerplate for Pydantic models, MotherDuck/DuckDB connector setup, and dbt model structure
- **Debugging**: identifying root causes of errors (e.g. MotherDuck date casting, S3 region configuration, dbt profile parsing)
- **Code review**: checking for security issues such as SQL injection vulnerabilities and suggesting fixes
- **Documentation**: drafting inline comments and sections of this README

All AI-generated code was reviewed, tested, and validated by the author before being committed. The system architecture, design decisions, keyword taxonomy, and analytical outputs are the author's own work. The author takes full responsibility for the correctness and integrity of the codebase.

---

*Built for the University of Sheffield IT Services team.*
