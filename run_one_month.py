import subprocess
import argparse
import multiprocessing
import os
import sys

def run_one_month(month, year):
    collectors = ["rrc10", "rrc00", "route-views2", "route-views.linx"]
    collectorsStr = ""
    for collector in collectors:
        collectorsStr += collector
        if not collector == collectors[-1]:
            collectorsStr += ","

    # Load the ribs/updates data into the kafka topics for one collector
    for collector in collectors:
        cmd = "KAFKA_HOST=localhost:9092 python produce_bgpdata.py -t ribs --collector " + collector + " --year " + str(
            year) + " --month " + str(month)
        subprocess.call(cmd, shell=True)  # returns the exit code in unix
        cmd = "KAFKA_HOST=localhost:9092 python produce_bgpdata.py -t updates --collector " + collector + " --year " + str(
            year) + " --month " + str(month)
        subprocess.call(cmd, shell=True)

    # Produce bgpatom and weighted_atom for one collector
    for collector in collectors:
        cmd = "python3 produce_bgpatom.py -c " + collector + " --year " + str(year) + " --month " + str(month)
        subprocess.call(cmd, shell=True)
        cmd = "python3 produce_weightedatom.py -l " + collector + " --year " + str(year) + " --month " + str(month)
        subprocess.call(cmd, shell=True)

    # Produce bc_score for all of the collectors
    for collector in collectors:
        cmd = "python3 produce_bcscore.py -c " + collector + " --year " + str(year) + " --month " + str(month)
        subprocess.call(cmd, shell=True)

    # Calculate the hegemony scores
    cmd = "python3 produce_hege.py --year " + str(year) + " --month " + str(month) + " --collectors " + collectorsStr
    subprocess.call(cmd, shell=True)

    # download the hege scores for this time period to a csv file
    cmd = "python3 save_hege_scores.py --year " + str(year) + " --month " + str(month)
    subprocess.call(cmd, shell=True)



def main() -> None:
    """Command line main function."""

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--start_year",
        type=int,
        default=2012
    )
    ap.add_argument(
        "--end_year",
        type=int,
        default=2022
    )    
    ap.add_argument(
        "--start_month",
        type=int,
        default=1
    )    
    ap.add_argument(
        "--end_month",
        type=int,
        default=12
    )
    ap.add_argument(
        "-p",
        "--parallel"
        type=int,
        default=6
    )
    args = ap.parse_args()

    years = range(args.start_year, args.end_year+1)
    months = range(args.start_month, args.end_month+1)

    poollist = []

    for month in months:
        for year in years:
            temptuple = [year, month]
            poollist.append(temptuple)

    with multiprocessing.Pool(processes=args.parallel) as pool:
        for result in pool.map(run_one_month, poollist):
            print("IHR_HEGE " + str(year) + ' ' + str(month) + " produced")
        pool.close()

if __name__ == "__main__":
    main()
