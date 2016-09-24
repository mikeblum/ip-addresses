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

# Redis RESP types
REDIS_TYPE_SIMPLE_STR = "+"
REDIS_TYPE_BULK_STR = "$"
REDIS_TYPE_INT = ":"
REDIS_TYPE_ARR = "*"
REDIS_TYPE_ERR = "-"

def get_redis_connection():
    r = redis.Redis(
        host='127.0.0.1',
        port=6379, 
        password='Nxp+_SN`:m[EeQmk-%{nUn0uIQ.m=OSJ'
    )
    return r

def gen_redis_proto(args):
    """
        Convert a Redis command to it's protocol representration
        
        args - a redis command in array form

        copied from the ruby implementattion:
        http://redis.io/topics/mass-insert
    """
    proto = ""
    proto += "*{}\r\n".format(len(args))
    for arg in args:
        # import pdb; pdb.set_trace()
        proto += "{}{}\r\n".format(REDIS_TYPE_BULK_STR, len(str(arg)))
        proto += "{}\r\n".format(arg)
    return proto

def ingest_ip_addresses():
    # r = get_redis_connection()
    with open('GeoLite2-City-Blocks-IPv4.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        cidrs = open('cidrs.txt', 'w')
        next(reader, None)  # skip the headers
        log.info('loaded IPV4 network data')
        index = 1
        t0 = time.time()
        for row in reader:
            cidr = row[0]
            # index cidr metadata by their cidrs as the key
            hkey = 'cidr:{}'.format(index)
            metadata = {
                'cidr': cidr,
                'geoid': row[1],
                'network': int(IPNetwork(cidr)[0]),
                'broadcast': int(IPNetwork(cidr)[-1]),
                'lat': str(row[7]),
                'long': str(row[8]),
                'range': int(row[9])
            }
            # add this cidr to the cidr:index - keyed by the sequential id
            # and max ip address range (broadcast address)
            cidrs.write(gen_redis_proto([
                'zadd', 
                'cidr:index', 
                metadata['broadcast'],
                index
            ]))
            # create record for this cidr
            # convert to an args list
            redis_cmd = ['hmset', hkey]
            for key in metadata.keys():
                redis_cmd.append(key)
                redis_cmd.append(metadata[key])
            cidrs.write(gen_redis_proto(redis_cmd))
            index += 1
            # log.debug('{} <-> {}'.format(network_addr, broadcast_addr))
        log.info('done in {} secs'.format(str(time.time() - t0)))
        # close output file
        cidrs.close()

if __name__ == '__main__':
    ingest_ip_addresses()
