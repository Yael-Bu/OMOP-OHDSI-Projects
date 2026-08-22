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
export DB_USER=""

export DB_HOST=""
export DB_NAME=""
export DB_PORT=""

# הרצת קוד האימון
python behrt_train.py