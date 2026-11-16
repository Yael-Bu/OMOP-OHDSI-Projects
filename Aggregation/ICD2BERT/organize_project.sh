#!/bin/bash
set -e

# מעבר לתיקיית pipeline במידה והרצת מתוך תיקיית האב ICD2BERT
if [ -d "pipeline" ] && [ -f "pipeline/run_experiments_array.sbatch" ]; then
    cd pipeline
fi

if [ ! -f "run_experiments_array.sbatch" ]; then
    echo "Error: Please run this script from ICD2BERT or ICD2BERT/pipeline"
    exit 1
fi

echo "=== 1. Creating clean directory structure inside $(pwd) ==="
mkdir -p src scripts analysis_plots archive_original_repo/old_src archive_original_repo/old_sbatch

# 2. העברת הקבצים הישנים מהריפו המקורי (../src ו-../run_*.sh) לארכיון מסודר
if [ -d "../src" ]; then
    echo "Moving legacy repo files from ../src to archive_original_repo/old_src/..."
    mv ../src/* archive_original_repo/old_src/ 2>/dev/null || true
    rmdir ../src 2>/dev/null || true
fi

for legacy_file in ../run_experiments.sh ../run_pipeline.sbatch run_experiments.sh run_pipeline.sbatch; do
    if [ -f "$legacy_file" ]; then
        echo "Archiving legacy script: $legacy_file"
        mv "$legacy_file" archive_original_repo/old_sbatch/
    fi
done

# 3. הסרת קובץ הגרפים הזמני שהוחלף
if [ -f "plot_all_benchmark_results.py" ]; then
    rm -f plot_all_benchmark_results.py
    echo "Removed temporary plot_all_benchmark_results.py"
fi

# 4. ניקוי קבצי קאש מיותרים של פייתון
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

echo "=== 2. Project organization completed! Current structure: ==="
ls -la
echo "--- Active Python modules in src/ ---"
ls -la src/
