"""
config.py — Central configuration for the UKRI Early Warning System.

All settings live here. Secrets are loaded from environment variables (see .env.example).
Edit KEYWORD_TAXONOMY to tune what technology signals get detected.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── API ────────────────────────────────────────────────────────────────────────
BASE_URL    = "https://gtr.ukri.org/gtr/api"
API_HEADERS = {"Accept": "application/vnd.rcuk.gtr.json-v7"}  # pin schema version (Joe's advice)
RETRY_MAX   = 3
RETRY_DELAY = 2     # seconds between retries (exponential backoff applied)
POLITE_DELAY = 0.25 # seconds between page requests — be a good API citizen

# ── Funder filter ─────────────────────────────────────────────────────────────
# There is no funder filter endpoint in the UKRI API (v7).
# We fetch ALL projects and filter client-side by this field.
TARGET_FUNDER = "Innovate UK"

# ── Storage: local vs S3 ──────────────────────────────────────────────────────
USE_S3      = os.getenv("USE_S3", "false").lower() == "true"
S3_BUCKET   = os.getenv("S3_BUCKET", "ukri-early-warning")
S3_PREFIX   = os.getenv("S3_PREFIX", "raw/ukri_pages")
LOCAL_CACHE = Path(os.getenv("LOCAL_CACHE", "cache/ukri_pages"))
OUTPUT_DIR  = Path(os.getenv("OUTPUT_DIR", "outputs"))

# ── Snowflake ─────────────────────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "UKRI_EWS")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
SNOWFLAKE_TABLE     = os.getenv("SNOWFLAKE_TABLE", "UKRI_PROJECTS")
RPI_SNOWFLAKE_TABLE = os.getenv("RPI_SNOWFLAKE_TABLE", "UKRI_ALL_PROJECTS")

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_RAW    = os.getenv("KAFKA_TOPIC_RAW",    "ukri.projects.raw")
KAFKA_TOPIC_TAGGED = os.getenv("KAFKA_TOPIC_TAGGED",  "ukri.projects.tagged")
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID",      "ukri-ews-consumer")

# ── Keyword taxonomy ──────────────────────────────────────────────────────────
# Keys become the 'category' column in the output.
# Keywords are matched (case-insensitive) against the combined text blob:
#   abstractText + techAbstractText + potentialImpact
#
# Design note (Joe's feedback): search all three fields together.
# Use techAbstractText and potentialImpact — they often contain more precise
# infrastructure signals than the general abstract.
KEYWORD_TAXONOMY: dict[str, list[str]] = {
    "ML_AI": [
        "machine learning", "deep learning", "neural network", "artificial intelligence",
        "large language model", "llm", "gpt", "transformer", "pytorch", "tensorflow",
        "keras", "reinforcement learning", "computer vision", "nlp",
        "natural language processing", "generative ai", "foundation model",
        "diffusion model", "bert", "gpu training", "image recognition",
        "object detection", "semantic segmentation", "automl", "federated learning",
        "stable diffusion", "text-to-image", "vision model",
    ],
    "HPC_Simulation": [
        "slurm", "mpi", "parallel computing", "high performance computing", "hpc",
        "cluster computing", "computational fluid dynamics", "cfd",
        "molecular dynamics", "finite element", "monte carlo", "distributed computing",
        "supercomputer", "petascale", "exascale", "openmp", "cuda",
        "gpu computing", "grid computing", "lattice boltzmann",
        "ab initio", "density functional theory", "dft", "quantum chemistry",
        "multiscale modelling", "numerical simulation",
    ],
    "Data_Intensive": [
        "big data", "genomics", "sequencing", "rna-seq", "whole genome",
        "large-scale", "petabyte", "terabyte", "high-throughput",
        "bioinformatics", "proteomics", "metabolomics",
        "data pipeline", "real-time data", "streaming data",
        "remote sensing", "satellite imagery", "sensor data",
        "internet of things", "time series", "image analysis", "data lake",
    ],
    "Cloud_Infrastructure": [
        "aws", "azure", "google cloud", "gcp", "kubernetes", "docker",
        "containerisation", "containerization", "cloud computing", "cloud storage",
        "serverless", "microservices", "infrastructure as code", "terraform",
        "cloud-native", "hybrid cloud", "object storage", "s3", "ec2", "lambda",
        "managed service", "virtual machine",
    ],
}
