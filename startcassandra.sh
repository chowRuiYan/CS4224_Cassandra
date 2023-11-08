#!/bin/sh
#SBATCH --time=480

srun nohup ~/apache-cassandra-4.1.3/bin/cassandra -f &
srun tail -f logs/system.log

