import argparse
import logging
import os

from hege.utils.config import Config

if __name__ == "__main__":
    text = """This script consumes all collectors bcscore and produce
    as hegemony score for --month and --year."""

    parser = argparse.ArgumentParser(description=text)
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
    args = parser.parse_args()
    
    assert args.year and args.month
    month_string = str(args.month)
    if len(month_string) < 2:
        month_string = "0" + month_string
    start_time_string = str(args.year)+"-"+month_string+"-01T00:00:00"
    end_time_string = str(args.year) + "-" + month_string + "-02T00:00:00"

    selected_collectors = list(map(lambda x: x.strip(), args.collectors.split(",")))
    Config.load(args.config_file)

    if args.prefix:
        log_filename_suffix = "prefix"
    else:
        log_filename_suffix = "asn"

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logDir = '/log/'
    if not os.path.exists(logDir):
        logDir = './'
    logging.basicConfig(
        format=FORMAT, filename=f"{logDir}/ihr-kafka-hegemony-{start_time_string}-{log_filename_suffix}.log",
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    # import after the config parameters are fully loaded
    from hege.hegemony.hege_builder import HegeBuilder
    from hege.utils.data_producer import DataProducer
    from hege.utils import utils

    start_ts = utils.str_datetime_to_timestamp(start_time_string)
    end_ts = utils.str_datetime_to_timestamp(end_time_string)

    hege_builder = HegeBuilder(selected_collectors, start_ts, end_ts, start_year, start_month, args.prefix, args.partition_id, args.sparse_peers)
    hege_data_producer = DataProducer(hege_builder)
    hege_data_producer.produce_kafka_messages_between()
