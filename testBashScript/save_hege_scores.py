
import argparse


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server', default='localhost:9092')
    parser.add_argument('-p', '--partitions', default=0)
    parser.add_argument('-m', '--month', default='2020')
    parser.add_argument('-y', '--year', default='1')

    args = parser.parse_args()
    print("Save hege scores for " + str(args.month) + " " + str(args.year))
