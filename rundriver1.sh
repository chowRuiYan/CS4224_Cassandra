#!/bin/sh
#SBATCH --time=180
#SBATCH --ntasks=8
#SBATCH --mem=16384MB

TMPDIR=`mktemp -d`
cp -r ~/CS4224_Cassandra $TMPDIR
cp -r ~/project_files $TMPDIR
cd $TMPDIR
nohup python CS4224_Cassandra/driver.py 0 & 
nohup python CS4224_Cassandra/driver.py 5 &
nohup python CS4224_Cassandra/driver.py 10 &
nohup python CS4224_Cassandra/driver.py 15 &

wait %1 %2 %3 %4

cp $TMPDIR/*.csv ~/jobresults/
rm -rf $TMPDIR/
