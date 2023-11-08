#!/bin/sh
#SBATCH --time=180
#SBATCH --ntasks=8
#SBATCH --mem=16384MB

TMPDIR=`mktemp -d`
cp -r ~/CS4224_Cassandra $TMPDIR
cp -r ~/project_files $TMPDIR
#echo $TMPDIR
cd $TMPDIR
nohup python CS4224_Cassandra/driver.py 3 & 
nohup python CS4224_Cassandra/driver.py 8 &
nohup python CS4224_Cassandra/driver.py 13 &
nohup python CS4224_Cassandra/driver.py 18 &

wait %1 %2 %3 %4

cp $TMPDIR/*.csv ~/jobresults/
rm -rf $TMPDIR/teamp-cassandra/apache-cassandra-4.1.3
