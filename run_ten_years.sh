#!/bin/bash

# immediately exit if there's an error
set -e

collectors=("rrc10", "rrc00", "route-views2", "route-views.linx")
start_year=2013
start_month=1
end_year=2023
end_month=12

# TODO: This is not multithreading (or processing or something)

for month in {1..12}; do
  for year in $(seq $start_year $end_year); do
    echo "$year $month"
#    if [[ $year -eq $end_year && $month > $end_month]]; then
#      continue
#    fi

    # Load the ribs/updates data into the kafka topics for one collector
    for collector in ${collectors[@]}; do
      KAFKA_HOST=localhost:9092 python produce_bgpdata.py -t ribs --collector $collector --year $year --month $month
      KAFKA_HOST=localhost:9092 python produce_bgpdata.py -t updates --collector $collector --year $year --month $month
    done

    # Produce bgpatom and weighted_atom for one collector
    for collector in ${collectors[@]}; do
      python3 produce_bgpatom.py -c $collector --year $year --month $month
      python3 produce_weightedatom.py -l $collector --year $year --month $month
    done

    # Produce bc_score for all of the collectors
    for collector in ${collectors[@]}; do
      python3 produce_bcscore.py -c $collector --year $year --month $month
    done

    # Calculate the hegemony scores
    for collector in ${collectors[@]}; do
      python3 produce_hege.py --year $year --month $month -c $collector
    done

    # download the hege scores for this time period to a csv file
    python3 save_hege_scores.py --year $year --month $month
  done
done