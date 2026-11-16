#!/bin/bash
#SBATCH --partition=main
#SBATCH --time=1-00:00:00
#SBATCH --job-name=behrt_benchmark_array
#SBATCH --output=logs/benchmark_%A_%a.out
#SBATCH --error=logs/benchmark_%A_%a.err
#SBATCH --mail-user=yaelbu@post.jce.ac.il
#SBATCH --mail-type=ALL
#SBATCH --gpus=1
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8
#SBATCH --array=0-2%3

# הדפסה מיידית של לוגים ללא חוצץ
export PYTHONUNBUFFERED=1

# מניעת פרגמנטציה (קיטוע) בזיכרון ה-GPU של PyTorch
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

# עצירה מיידית במקרה של שגיאה
set -e

# יצירת תיקיות תשתית אם אינן קיימות
mkdir -p logs data results

echo "==================== Job Information ===================="
echo "Date               : $(date)"
echo -e "SLURM_JOBID        :\t $SLURM_JOB_ID"
echo -e "SLURM_ARRAY_TASK_ID:\t $SLURM_ARRAY_TASK_ID"
echo -e "SLURM_JOB_NODELIST :\t $SLURM_JOB_NODELIST"
echo "=========================================================="

# טעינה ואתחול תקני של סביבת הקונדה
module load anaconda
eval "$(conda shell.bash hook)"
conda activate behrt_env

# הגדרת פרטי התחברות ל-PostgreSQL בשרת המרוחק
export DB_USER="omop_user"
export DB_HOST="172.16.103.15"
export DB_NAME="omop_med_db"
export DB_PORT="5432"

MAPPING_FILE="mapping_omop_concept_ids.csv"
DATA_DIR="data"

# מנגנון נעילה למניעת Race Condition: המשימה הראשונה מייצאת, האחרות ממתינות
LOCK_DIR="$DATA_DIR/.export_lock"
if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "--- Task $SLURM_ARRAY_TASK_ID: Acquired export lock. Checking tables... ---"

    # ייצוא טבלאות OMOP מתוך סכמת omop (כולל death עבור נתון אמת של תמותה)
    for table in condition_occurrence visit_occurrence death; do
        if [ ! -s "$DATA_DIR/${table}.csv" ]; then
            echo "Exporting OMOP table: ${table} (schema: omop)..."
            psql -U $DB_USER -h $DB_HOST -d $DB_NAME -p $DB_PORT \
              -c "\copy omop.${table} TO STDOUT WITH CSV HEADER" > "$DATA_DIR/${table}.csv" 2>/dev/null || true
        fi
    done

    # ייצוא טבלאות MIMIC מתוך סכמת mimiciv_hosp
    for table in diagnoses_icd admissions; do
        if [ ! -s "$DATA_DIR/${table}.csv" ]; then
            echo "Exporting MIMIC table: ${table} (schema: mimiciv_hosp)..."
            psql -U $DB_USER -h $DB_HOST -d $DB_NAME -p $DB_PORT \
              -c "\copy mimiciv_hosp.${table} TO STDOUT WITH CSV HEADER" > "$DATA_DIR/${table}.csv" 2>/dev/null || true
        fi
    done

    # יצירת קישורים סימבוליים להתאמה מלאה למבנה הטבלאות
    [ -s "$DATA_DIR/diagnoses_icd.csv" ] && [ ! -e "$DATA_DIR/diagnosis.csv" ] && ln -sf diagnoses_icd.csv "$DATA_DIR/diagnosis.csv"
    [ -s "$DATA_DIR/admissions.csv" ] && [ ! -e "$DATA_DIR/edstays.csv" ] && ln -sf admissions.csv "$DATA_DIR/edstays.csv"

    rm -rf "$LOCK_DIR"
    touch "$DATA_DIR/.export_done"
    echo "--- Export step finished successfully. ---"
else
    echo "--- Task $SLURM_ARRAY_TASK_ID: Waiting for primary task to complete export... ---"
    while [ ! -f "$DATA_DIR/.export_done" ]; do
        sleep 5
    done
    echo "--- Primary export finished. Continuing execution. ---"
fi

# מיפוי משימות המערך (Array Tasks)
# 0 -> mimic (שחזור המאמר המקורי על קודי ICD)
# 1 -> omop_raw (סנומד גולמי ללא אגרגציה)
# 2 -> omop_aggregated (סנומד עם אגרגציה בזמן אמת בזיכרון)
MODES=("mimic" "omop_raw" "omop_aggregated")
CURRENT_MODE=${MODES[$SLURM_ARRAY_TASK_ID]}

EXTRA_ARGS=""
if [ "$CURRENT_MODE" == "omop_aggregated" ]; then
    if [ ! -f "$MAPPING_FILE" ] && [ -f "$DATA_DIR/$MAPPING_FILE" ]; then
        MAPPING_FILE="$DATA_DIR/$MAPPING_FILE"
    fi

    if [ ! -f "$MAPPING_FILE" ]; then
        echo "Error: Mapping CSV '$MAPPING_FILE' not found!"
        exit 1
    fi
    echo "Using existing aggregation mapping: $MAPPING_FILE"
    EXTRA_ARGS="--mapping_csv $MAPPING_FILE"
fi

echo ">>> Launching train_pipeline.py for mode: $CURRENT_MODE (60 epochs)"
python -u src/train_pipeline.py \
    --mode "$CURRENT_MODE" \
    --data_dir "$DATA_DIR" \
    $EXTRA_ARGS \
    --epochs 60 \
    --batch_size 64 \
    --output_dir results

echo "=========================================================="
echo "Task $SLURM_ARRAY_TASK_ID ($CURRENT_MODE) completed at: $(date)"
echo "=========================================================="

# הדפסת סיכום התוצאות מתוך הקובץ המרכז
if [ -f "results/benchmark_summary.txt" ]; then
    echo "--- Current Benchmark Summary ---"
    cat results/benchmark_summary.txt
    echo "---------------------------------"
fi