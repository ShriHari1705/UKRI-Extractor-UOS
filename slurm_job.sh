#!/bin/bash
#SBATCH --job-name=ukri-ews
#SBATCH --output=logs/ukri_ews_%j.log
#SBATCH --error=logs/ukri_ews_%j.err
#SBATCH --time=08:00:00          # full run (~8660 pages) takes ~3-4 hrs; 8hr buffer
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1        # single-threaded fetch (polite to the API)
#SBATCH --partition=general      # adjust to your cluster partition name
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=your@email.ac.uk

# ── UKRI Early Warning System — SLURM Batch Job ───────────────────────────────
# Run on Stanage / Bessemer:
#   sbatch slurm_job.sh
#
# For a dev test (first 10 pages only):
#   sbatch --export=ALL,MAX_PAGES=10 slurm_job.sh

set -euo pipefail

echo "======================================"
echo " UKRI Early Warning System"
echo " Job ID:  $SLURM_JOB_ID"
echo " Node:    $SLURMD_NODENAME"
echo " Started: $(date)"
echo "======================================"

# Load Python module (adjust module name for your cluster)
module load Python/3.11.3-GCCcore-12.3.0

# Activate virtual environment
REPO_DIR="$HOME/ukri-early-warning"
source "$REPO_DIR/.venv/bin/activate"

cd "$REPO_DIR"
mkdir -p logs

# Run the pipeline
# Set MAX_PAGES env var to limit pages (useful for testing)
if [ -n "${MAX_PAGES:-}" ]; then
    echo "DEV MODE: limiting to $MAX_PAGES pages"
    python run_pipeline.py --pages "$MAX_PAGES" --motherduck
else
    python run_pipeline.py --motherduck
fi

echo "======================================"
echo " Pipeline finished: $(date)"
echo "======================================"
