import sys
import msgpack
import argparse
import csv
import os

from confluent_kafka import Consumer, TopicPartition, KafkaError

"""
This should copy all of the hegemony scores from the topic ihr_hegemony_yyyy_mm to a file called hegescores/hegePerASN_yyyy-mm.csv
"""

# populates a csv file '.../hegePerASN_yyyy-mm.csv' with a list of {asn, hege} values
def downloadDataToFile(server, num_partitions, month, year, datapath):
    topic = "ihr_hegemony_"+year+"_"+month
    filename = datapath + "/hegePerASN" + "_" + year + "-" + month + ".csv"

    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'ihr_tail',  # does this need to be named this?
        'enable.auto.commit': False,
    })

    partition = TopicPartition(topic, num_partitions)
    low, high = consumer.get_watermark_offsets(partition)
    partition = TopicPartition(topic, num_partitions, low)
    consumer.assign([partition])

    with open(filename, 'a', newline='') as file:
        file.truncate(0)  # delete old contents
        writer = csv.writer(file)
        i = 0
        while True:
            msg = consumer.poll(1000)
            msgdict = {
                'topic': msg.topic(),
                'partition': msg.partition(),
                'key': msg.key(),
                'timestamp': msg.timestamp(),
                'headers': msg.headers(),
                'value': msgpack.unpackb(msg.value(), raw=False)
            }


            data = msgdict['value']
            writer.writerow([data['asn'], data['hege']])

            i += 1
            if i >= high:
                break

        consumer.close()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server', default='localhost:9092')
    parser.add_argument('-p', '--partitions', default=0)
    parser.add_argument('-m', '--month', default='2020')
    parser.add_argument('-y', '--year', default='1')

    args = parser.parse_args()
    monthStr = str(args.month)
    if len(monthStr) < 2:
        monthStr = '0' + monthStr

    datapath = "./hegescores"
    # create folder for hegemony data
    dataFolder = os.path.exists(datapath)
    if not dataFolder:
        os.mkdir(datapath)


    # searchByASNKafka(args.asn, args.topic, args.partitions, args.server)
    downloadDataToFile(args.server, args.partitions, monthStr, str(args.year), datapath)
