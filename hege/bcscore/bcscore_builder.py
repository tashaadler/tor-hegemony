from collections import defaultdict
import json
import math

from hege.bgpatom.bgpatom_loader import BGPAtomLoader
from hege.bcscore.viewpoint import ViewPoint
from hege.utils import utils
from hege.utils.config import Config

DUMP_INTERVAL = Config.get("bcscore")["dump_interval"]
AS_BCSCORE_META_DATA_TOPIC = Config.get("bcscore")["meta_data_topic__as"]
AS_BCSCORE_DATA_TOPIC = Config.get("bcscore")["data_topic__as"]
PREFIX_BCSCORE_META_DATA_TOPIC = Config.get("bcscore")["meta_data_topic__prefix"]
PREFIX_BCSCORE_DATA_TOPIC = Config.get("bcscore")["data_topic__prefix"]

DATA_BATCH_SIZE = 10000


class BCScoreBuilder:
    def __init__(self, collector: str, start_timestamp: str, end_timestamp: str, year: int, month: int, 
            prefix_mode=False, address_family=4):
        self.collector = collector
        self.year = year
        self.month = month
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.prefix_mode = prefix_mode
        self.address_family = address_family
        yearstr = str(self.year)
        if month <10:
            monthstr = '0' + str(self.month)
        else:
            monthstr = str(self.month)
        if prefix_mode:
            self.kafka_data_topic = f"{PREFIX_BCSCORE_DATA_TOPIC}_{collector}"
            self.kafka_meta_data_topic = f"{PREFIX_BCSCORE_META_DATA_TOPIC}_{collector}"
        else:
            self.kafka_data_topic = f"{AS_BCSCORE_DATA_TOPIC}_{collector}_{yearstr}_{monthstr}"
            self.kafka_meta_data_topic = f"{AS_BCSCORE_META_DATA_TOPIC}_{collector}_{yearstr}_{monthstr}"

    def consume_and_calculate(self):
        for current_timestamp in range(self.start_timestamp, self.end_timestamp, DUMP_INTERVAL):
            bgpatom = self.load_bgpatom(current_timestamp)
            yield current_timestamp, self.get_viewpoint_bcscore_generator(bgpatom, current_timestamp)

    def load_bgpatom(self, atom_timestamp, year, month):
        bgpatom = BGPAtomLoader(self.collector, atom_timestamp, year, month).load_data()
        return bgpatom

    def get_viewpoint_bcscore_generator(self, bgpatom: dict, atom_timestamp: int):
        peers_by_asn = defaultdict(list)
        for peer_address, peer_asn in bgpatom:
            peers_by_asn[peer_asn].append(peer_address)

        for peer_asn in peers_by_asn:
            peers_list = peers_by_asn[peer_asn]
            for message, scope in self.calculate_bcscore_per_asn(peers_list, peer_asn, bgpatom, atom_timestamp):
                yield message, scope

    def calculate_bcscore_per_asn(self, peers_in_asn_list: list, peer_asn: str, bgpatom: dict, atom_timestamp: int):
        sum_bcscore = defaultdict(lambda: defaultdict(int))
        peers_count = defaultdict(lambda: defaultdict(int))

        for peer_address in peers_in_asn_list:
            # Skip peers for wrong IP version
            if( ('.' in peer_address and self.address_family == 6) 
                    or (':' in peer_address and self.address_family == 4)): 
                continue
        
            peer_bgpatom = bgpatom[(peer_address, peer_asn)]
            peer_bcscore_generator = self.calculate_viewpoint_bcscore(peer_bgpatom, peer_address, atom_timestamp)
            for scope_bcscore, scope in peer_bcscore_generator:
                for depended_asn, depended_asn_bcscore in scope_bcscore.items():
                    sum_bcscore[scope][depended_asn] += depended_asn_bcscore
                    peers_count[scope][depended_asn] += 1

        for scope in sum_bcscore:
            bcscore_by_asn = dict()
            for depended_asn in sum_bcscore[scope]:
                bcscore_by_asn[depended_asn] = sum_bcscore[scope][depended_asn] / peers_count[scope][depended_asn]

            # Limit the number of asn (message size)
            bba_list = list(bcscore_by_asn.items())
            idx = range(len(bba_list))
            for bba_batch in idx[::DATA_BATCH_SIZE]:
                yield self.format_dump_data(dict(bba_list[bba_batch:bba_batch+DATA_BATCH_SIZE]), scope, peer_asn, atom_timestamp), scope

    def calculate_viewpoint_bcscore(self, bgpatom: dict, peer_address: str, atom_timestamp: int):
        viewpoint = ViewPoint(peer_address, self.collector, bgpatom, atom_timestamp, self.prefix_mode)
        return viewpoint.calculate_viewpoint_bcscore()

    @staticmethod
    def format_dump_data(bcscore_by_asn: dict, scope: str, peer_asn: str, dump_timestamp: int):
        return {
            "bcscore": bcscore_by_asn,
            "scope": scope,
            "peer_asn": peer_asn,
            "timestamp": dump_timestamp
        }


if __name__ == "__main__":
    start_at_time_string = "2020-08-01T00:00:00"
    start_at = utils.str_datetime_to_timestamp(start_at_time_string)

    end_at_time_string = "2020-08-01T00:01:00"
    end_at = utils.str_datetime_to_timestamp(end_at_time_string)

    test_collector = "rrc10"

    bcscore_builder = BCScoreBuilder(test_collector, start_at, end_at)
    for timestamp, viewpoint_bcscore_generator in bcscore_builder.consume_and_calculate():
        print(timestamp)
        for bcscore, test_scope in viewpoint_bcscore_generator:
            print(test_scope, bcscore)
            break
        break
