import argparse
import logging
from netaddr import IPNetwork
import pprint
import redis

def get_redis_connection():
    r = redis.Redis(
        host='127.0.0.1',
        port=6379, 
        password='redis'
    )
    return r

def get_info(address):
    logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
    log = logging.getLogger('query')

    pp = pprint.PrettyPrinter(indent=4)
    r = get_redis_connection()
    ipaddr = IPNetwork(address).ip
    zrange = r.zrangebyscore('cidr:index', int(ipaddr), '+inf', start=0, num=1)
    if len(zrange) == 0:
        log.warn('REDIS: no geodata found for {}'.format(address))
        return False
    for result in zrange:
        key = 'cidr:{}'.format(result)
        cidr_metadata = r.hgetall(key)
        geo_key = 'geoid:{}'.format(cidr_metadata['geoid'])
        geo_metadata = r.hgetall(geo_key)
    return True