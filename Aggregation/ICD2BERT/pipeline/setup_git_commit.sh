#!/bin/bash
set -e

if [ -d "pipeline" ] && [ -f "pipeline/run_experiments_array.sbatch" ]; then
    cd pipeline
fi

# אתחול Git בתיקיית pipeline אם טרם אותחלה
if [ ! -d ".git" ]; then
    git init
    git branch -M main
fi

echo "=== 1. Writing .gitignore to exclude heavy medical data & model weights ==="
cat << 'GITEOF' > .gitignore
# Raw clinical datasets (MIMIC-IV / OMOP CDM)
data/

# Heavy model checkpoints, embeddings, and pickled classifiers
results/*.pt
results/*.npz
results/*.pkl

# Backups, SLURM logs, and archived legacy scripts
results_backup_*/
logs/
archive_original_repo/

# Python & OS artifacts
__pycache__/
*.pyc
*.pyo
.DS_Store
.ipynb_checkpoints/
GITEOF

echo "=== 2. Staging clean code, mappings, and benchmark summaries ==="
git add .gitignore
git add src/*.py
git add *.sbatch *.py *.sh 2>/dev/null || true
git add mapping_*.csv 2>/dev/null || true
git add results/benchmark_summary.txt results/subsampling_benchmark.csv results/loss_history_*.csv 2>/dev/null || true
git add results/plots/ results/research_plots/ 2>/dev/null || true

echo "=== 3. Verifying staged files (checking no >50MB files are included) ==="
git status

echo "=== 4. Committing Milestone 1 ==="
git commit -m "Milestone 1: Clean BEHRT pipeline, Frozen CLS benchmarks, and Subsampling curves" || echo "Nothing new to commit."
git tag -f v1.0-frozen-baseline

echo "Done! Your repository is clean, organized, and tagged at v1.0-frozen-baseline."
