import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--month", "-m", help="Choose the month")
    parser.add_argument("--year", "-y", help="Choose the year")
    parser.add_argument("--collectors", "-c",
                        help="Choose your collectors: it should be written in the following patterns"
                             "collector1,collector2,collector3")
    parser.add_argument("--prefix", "-p",
                        help="With this flag, the script will run in prefix hege mode",
                        action='store_true')
    parser.add_argument("--partition_id", help="Select only one kafka partition")
    parser.add_argument("--sparse_peers", help="Do not assume full-feed peers",
                        action='store_true')
    parser.add_argument("--config_file", "-C",
                        help="Path to the configuration file")

    args = parser.parse_args()

    assert args.year and args.month
    print("hege " + str(args.collectors) + ", " + str(args.year) + " " + str(args.month))

