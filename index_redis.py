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
        password='redis'
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
    with open('GeoLite2-City-Blocks-IPv4.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        cidrs = open('migration.txt', 'w')
        next(reader, None)  # skip the headers
        log.info('loaded IPV4 network data')
        t0 = time.time()
        for row in reader:
            cidr = row[0]
            # index cidr metadata by their cidrs as the key
            hkey = 'cidr:{}'.format(cidr)
            metadata = {
                'geoid': row[1],
                'network': int(IPNetwork(cidr)[0]),
                'broadcast': int(IPNetwork(cidr)[-1]),
                'lat': str(row[7]),
                'long': str(row[8])
            }
            # add this cidr to the cidr:index - keyed by the sequential id
            # and max ip address range (broadcast address)
            cidrs.write(gen_redis_proto([
                'zadd', 
                'cidr:index', 
                metadata['broadcast'],
                cidr
            ]))
            # create record for this cidr
            # convert to an args list
            redis_cmd = ['hmset', hkey]
            for key in metadata.keys():
                redis_cmd.append(key)
                redis_cmd.append(metadata[key])
            cidrs.write(gen_redis_proto(redis_cmd))
        # close output file
        cidrs.close()

def ingest_city_locations():
    with open('GeoLite2-City-Locations-en.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)  # skip the headers
        log.info('loaded city location data')
        geoids = open('migration.txt', 'a')
        t0 = time.time()
        for row in reader:
            geoid = row[0]
            hkey = 'geoid:{}'.format(geoid)
            metadata = {
                'city': row[10],
                'country': row[5]
            }
            # create record for this cidr
            # convert to an args list
            redis_cmd = ['hmset', hkey]
            for key in metadata.keys():
                redis_cmd.append(key)
                redis_cmd.append(metadata[key])
            geoids.write(gen_redis_proto(redis_cmd))
        geoids.close()


if __name__ == '__main__':
    ingest_ip_addresses()
    ingest_city_locations()
