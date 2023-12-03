#!/bin/bash

# immediately exit if there's an error
set -e

# TODO: are we running all of the times at once? Or one month at a time?

# collectors: rrc10, rrc00, route-views2, route-views.linx
collectors=("rrc10", "rrc00", "route-views2", "route-views.linx")
start_time = '2013-11-01T00:00:00'
end_time = '2023-11-01T00:00:00'

# Load ribs/updates data into the kafka topics for all collectors
# TODO: put bgpstream2.py in this directory
for collector in ${collectors[@]}; do
  KAFKA_HOST=localhost:9092 python bgpstream2.py -t ribs --collector $collector --startTime $start_time --endTime $end_time
  KAFKA_HOST=localhost:9092 python bgpstream2.py -t updates --collector $collector --startTime $start_time --endTime $end_time
done

# Produce bgpatom and weighted_atom for all of the collectors
for collector in ${collectors[@]}; do
  python3 produce_bgpatom.py -c $collector -s $start_time -e $end_time
  python3 produce_weightedatom.py -c $collector -s $start_time -e $end_time
done

# Produce bc_score for all of the collectors
for collector in ${collectors[@]}; do
  python3 produce_bcscore.py -c $collector -s $start_time -e $end_time
done

# Calculate the hegemony scores
python3 produce_hege.py -s $start_time -e $end_time -c rrc00,rrc10,route-views.linx,route-views2

# run