#!/bin/bash

# immediately exit if there's an error
set -e

# TODO: are we running all of the times at once? Or one month at a time?

# collectors: rrc10, rrc00, route-views2, route-views.linx
collectors=("rrc10", "rrc00", "route-views2", "route-views.linx")
start_time='2013-11-01T00:00:00'
end_time='2023-11-01T00:00:00'


# Load ribs/updates data into the kafka topics for all collectors
for collector in ${collectors[@]}; do
  KAFKA_HOST=localhost:9092 python produce_bgpdata.py -t ribs --collector $collector --startTime $start_time --endTime $end_time
  KAFKA_HOST=localhost:9092 python produce_bgpdata.py -t updates --collector $collector --startTime $start_time --endTime $end_time
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

# Run everything for each year and month
# TODO: is this remotely close to the correct syntax?
start_year=2013
end_year=2023
for month in {01..12}; do
  for year in {start_year..end_year}; do
    start_time="$year-$month-01T00:00:00"
    end_month=((month+1))
    end_year = year
    if [ $((end_month == 13)) ]; then
      end_month=01
      end_year=((year+1))
    fi
    end_time="$end_year-$end_month-01T00:00:00"

    # Calculate the hegemony scores
    python3 produce_hege.py -s $start_time -e $end_time -c rrc00,rrc10,route-views.linx,route-views2

    # download the hege scores for this time period
  done
done


# run