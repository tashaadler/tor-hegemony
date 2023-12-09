import argparse
import logging
import os

from hege.utils.config import Config

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
     
    if args.month < 10:
        month = '0' + str(args.month)
    else:
        month = str(args.month)


    selected_collector = args.collector
    start_time_string = str(args.year)+"-"+str(month)+"-01T00:00:00"
    end_time_string = str(args.year) + "-" + str(month) + "-02T00:00:00"

    Config.load(args.config_file)

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logDir = '/log/'
    if not os.path.exists(logDir):
        logDir = './'
    logging.basicConfig(
        format=FORMAT, filename=f"{logDir}/ihr-kafka-bgpatom_{start_time_string}-{selected_collector}.log",
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    # import after the config parameters are fully loaded
    from hege.bgpatom.bgpatom_builder import BGPAtomBuilder
    from hege.utils.data_producer import DataProducer
    from hege.utils import utils

    start_ts = utils.str_datetime_to_timestamp(start_time_string)
    end_ts = utils.str_datetime_to_timestamp(end_time_string)

    bgpatom_builder = BGPAtomBuilder(selected_collector, start_ts, end_ts, start_year, start_month)
    bgpatom_data_producer = DataProducer(bgpatom_builder)
    bgpatom_data_producer.produce_kafka_messages_between()
