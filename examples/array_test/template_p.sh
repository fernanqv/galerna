#!/bin/bash
#SBATCH --array=0-62
#SBATCH --cpus-per-task=16
#SBATCH --partition=meteo_long
##SBATCH -o logs/job_%A_%a.out

#module load 2024
#module load parallel
#module load parallel/20240722-GCCcore-13.3.0


## DO NOT TOUCH
START_LINE=$(( SLURM_ARRAY_TASK_ID * 16 + 1 ))
END_LINE=$(( START_LINE + 4 - 1 ))

sed -n "${START_LINE},${END_LINE}p" commands.txt | parallel -j 16 'echo $(date); sleep 2; echo hola {}'
