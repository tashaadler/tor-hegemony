#!/bin/bash

# immediately exit if there's an error
set -e

while getopts y:m: flag
do
    case "${flag}" in
        y) year=${OPTARG};;
        m) month=${OPTARG};;
    esac
done

collectors=("rrc10", "rrc00", "route-views2", "route-views.linx")

echo "$year $month"

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