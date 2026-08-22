#!/bin/bash
#SBATCH --partition main
#SBATCH --job-name behrt_train
#SBATCH --output job-%j.out
#SBATCH --gpus=1
#SBATCH --cpus-per-task=4

# הדפסת פרטי ה-JOB לקובץ הפלט
echo "Date: $(date)"
echo -e "SLURM_JOBID:\t\t $SLURM_JOBID"
echo -e "SLURM_JOB_NODELIST:\t $SLURM_JOB_NODELIST"

# טעינת anaconda והפעלת הסביבה הווירטואלית
module load anaconda
source activate behrt_env

# הגדרת משתני הסביבה (פרטי התחברות ל-DB)
export DB_USER="omop_user"
export DB_PASS='&e%5UJb^Hv^L0d!.'
export DB_HOST="172.16.103.15"
export DB_NAME="omop_med_db"
export DB_PORT="5432"

# הרצת קוד האימון
python -u cohort_statistics_summary.py