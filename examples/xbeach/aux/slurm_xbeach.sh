#! /bin/bash
#SBATCH --job-name=xbeach   
#SBATCH --partition=geocean       
#SBATCH --nodes=1
#SBATCH --time=01:00:00

source /software/geocean/conda/bin/activate
conda activate xbeach

ROOT=/software/geocean/xbeach/slurm_netcdf_mpi
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ROOT/lib
export PATH=$ROOT/bin:$PATH

srun /software/geocean/xbeach/with-netcdf/bin/xbeach 
