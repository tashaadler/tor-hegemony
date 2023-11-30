import sys
import msgpack
import argparse
import csv

from confluent_kafka import Consumer, TopicPartition, KafkaError

"""
This should copy all of the hegemony scores to a file called hegemonydata.csv
"""

# populates a csv file 'hegemonydata.csv' with a list of {asn, hege} values
# not currently used
def downloadDataToFile(server, num_partitions, topic):

    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'ihr_tail',  # does this need to be named this?
        'enable.auto.commit': False,
    })

    partition = TopicPartition(topic, num_partitions)
    low, high = consumer.get_watermark_offsets(partition)
    partition = TopicPartition(topic, num_partitions, low)
    consumer.assign([partition])

    with open('hegemonydata.csv', 'a', newline='') as file:
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
    parser.add_argument('-t', '--topic', default='ihr_hegemony')

    args = parser.parse_args()

    # searchByASNKafka(args.asn, args.topic, args.partitions, args.server)
    downloadDataToFile(args.server, args.partitions, args.topic)