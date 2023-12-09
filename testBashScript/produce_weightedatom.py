import argparse

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', '--year', type=int, help='Year to produce weighted atom for')
    parser.add_argument('-m', '--month', type=int, help='Month to produce weighted atom for')
    parser.add_argument('-r', '--replication-factor', type=int,
                        help='replication factor for DEST (default: '
                             'replication factor of SOURCE)')
    parser.add_argument('-p', '--partitions', type=int,
                        help='number of partitions for DEST (default: number '
                             'of partitions of SOURCE)')
    parser.add_argument('-s', '--server', default='localhost:9092',
                        help='bootstrap server (default: localhost:9092)')
    config_group_desc = """By default, the script copies the configuration from
                            SOURCE. This behavior can be overridden by using the 
                            --default flag. New configuration parameters can be
                            passed in a JSON file and the --configuration option.
                            See 
                            https://kafka.apache.org/documentation.html#topicconfigs
                            for a list of valid parameters. These parameters take
                            precedence over parameters copies from SOURCE or 
                            --default."""
    config_group = parser.add_argument_group('Topic configuration',
                                             description=config_group_desc)
    config_group.add_argument('-d', '--default', action='store_true',
                              help='use default configuration settings for '
                                   'DEST')
    config_group.add_argument('-l', '--collector',
                              help='collector '
                                   'collector to copy from')
    config_group.add_argument('-c', '--configuration',
                              help='JSON file with new configuration '
                                   'parameters for DEST')

    args = parser.parse_args()
    print("hege " + str(args.collector) + ", " + str(args.year) + " " + str(args.month))


if __name__ == '__main__':
    main()
