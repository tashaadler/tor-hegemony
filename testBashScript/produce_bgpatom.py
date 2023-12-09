import argparse

if __name__ == "__main__":
    text = """This script consumes BGP Data from selected collector(s) 
    and produce bgpatom between --start_time and --end_time. It then 
    analyzes and publishes BGP atom to kafka cluster"""

    parser = argparse.ArgumentParser(description=text)
    parser.add_argument("--collector", "-c", help="Choose collector to push data for")
    parser.add_argument("--month", "-m", help="Choose the month")
    parser.add_argument("--year", "-y", help="Choose the year")
    parser.add_argument("--config_file", "-C",
                        help="Path to the configuration file")

    args = parser.parse_args()
    assert args.year and args.collector and args.month
     
    print("bgpatom " + args.collector +", "+args.year + " " + args.month)

