import argparse
"""This script was originally from the IHR kafka-toolbox repository, located at
/bgp/producers/bgpstream2.py. We have made minor edits."""

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--collector","-c",help="Choose collector to push data for")
    parser.add_argument("--year","-y",help="Choose the year (Format: yyyy; Example: 2017)")
    parser.add_argument("--month","-m",help="Choose the month (Format: mm or m; Example: 9)")
    parser.add_argument("--type","-t",help="Choose record type: ribs or updates")

    args = parser.parse_args() 

    print("bgpdata "+args.type+", " + args.collector +", "+args.year + " " + args.month)
