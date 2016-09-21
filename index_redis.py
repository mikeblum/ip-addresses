import csv
import logging
import os
import redis
import sys
import time
from netaddr import IPNetwork

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
log = logging.getLogger('setup_kdtree')

def get_redis_connection():
    r = redis.Redis(
        host='127.0.0.1',
        port=6379, 
        password='Nxp+_SN`:m[EeQmk-%{nUn0uIQ.m=OSJ'
    )
    return r

def ingest_ip_addresses():
    with open('GeoLite2-City-Blocks-IPv4.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)  # skip the headers
        log.info('loaded IPV4 network data')
        t0 = time.time()
        for row in reader:
            cidr = row[0]
            network_addr = IPNetwork(cidr)[0]
            broadcast_addr = IPNetwork(cidr)[-1]
            # log.debug('{} <-> {}'.format(network_addr, broadcast_addr))
        log.info('done in {} secs'.format(str(time.time() - t0)))

if __name__ == '__main__':
    r = get_redis_connection()
    print(r.ping())
    # ingest_ip_addresses()
