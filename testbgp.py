import subprocess
import argparse
import multiprocessing
import os
import sys

collector= "rrc10"
year = '2018'
month = '01'


cmd = "KAFKA_HOST=localhost:9092 python3 produce_bgpdata.py -t ribs --collector " + collector + " --year " + year + " --month " + month
print(cmd)
return_code = subprocess.call(cmd, shell=True)  # returns the exit code in unix
print('\n return code = ' + str(return_code))
cmd = "KAFKA_HOST=localhost:9092 python3 produce_bgpdata.py -t updates --collector " + collector + " --year " + year + " --month " + month
print(cmd)
return_code = subprocess.call(cmd, shell=True)  # returns the exit code in unix
print('\n return code = ' + str(return_code))
