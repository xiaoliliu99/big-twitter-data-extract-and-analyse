#!/bin/bash
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-00:10:00

module load mpi4py/3.0.2-timed-pingpong
module load python/3.7.4

srun -n 8 python3 As1.py

##Job monitor command to list the resource usage
my-job-stats -a -n -s