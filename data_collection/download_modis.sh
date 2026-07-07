#!/bin/bash
#SBATCH --job-name="modis_mcd64a1_download"
#SBATCH --partition=peregrine-cpu
#SBATCH --qos=cpu_short
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=24:00:00
#SBATCH --output=out_and_err/download_modis_%j.out
#SBATCH --error=out_and_err/download_modis_%j.err


# IMPORTANT for multiprocessing: keep BLAS/MKL single-threaded per process
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

srun python download_modis.py
