#!/usr/bin/env python

"""
Example of querying for an IP address, finding the associated CIDR rage
and from their the referenced geographic information about that IP address.
"""

import argparse
import logging
import time
from db.models import Location, IPV4_Network, IPV6_Network
from netaddr import IPNetwork
import pprint
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

DBSession = scoped_session(sessionmaker())

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
log = logging.getLogger('query')

def as_dict(record):
    return {c.name: getattr(record, c.name) for c in record.__table__.columns}

def main():
    pp = pprint.PrettyPrinter(indent=4)
    # creates a sqlite file if there isn't one
    engine = create_engine('sqlite:///{db_name}.sqlite'.format(db_name="geoip"),
                            encoding='utf8')

    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)

    parser = argparse.ArgumentParser(description='Fetch geodata for an IPv4 address.')
    parser.add_argument('addresses', nargs='+',
                    help='IPv4 addresses')
    args = parser.parse_args()
    try:
        for addr in args.addresses:
            ip_addr = IPNetwork(addr)
            print('CIDR: {}'.format(ip_addr.cidr))
            print('NETWORK: {}'.format(ip_addr.network))
            print('BROADCAST: {}'.format(ip_addr.broadcast))
            mask = int(ip_addr.ip)
            t0 = time.time()
            query = DBSession.query(IPV4_Network, Location).\
                        filter(mask >= IPV4_Network.network_address).\
                        filter(mask <= IPV4_Network.broadcast_address).\
                        join(Location, IPV4_Network.geoname_id == Location.geoname_id)
            if query.count() == 0:
                log.warn('no geodata found for {}'.format(addr))
            for result in query:
                for segment in result:
                    log.info(as_dict(segment))
            log.info('done in {} secs'.format(str(time.time() - t0)))
    except Exception as exp:
        log.error('Aborting query {}'.format(exp))
        DBSession.rollback()
    finally:
        DBSession.close()

if __name__ == '__main__':
    main()