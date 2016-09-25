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

def as_dict(record):
    return {c.name: getattr(record, c.name) for c in record.__table__.columns}

def get_info(address):
    logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
    log = logging.getLogger('query')

    pp = pprint.PrettyPrinter(indent=4)
    # creates a sqlite file if there isn't one
    engine = create_engine('sqlite:///{db_name}.sqlite'.format(db_name="geoip"),
                            encoding='utf8')
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        ip_addr = IPNetwork(address)
        mask = int(ip_addr.ip)
        query = DBSession.query(IPV4_Network, Location).\
                    filter(mask >= IPV4_Network.network_address).\
                    filter(mask <= IPV4_Network.broadcast_address).\
                    join(Location, IPV4_Network.geoname_id == Location.geoname_id)
        if query.count() == 0:
            log.warn('SQL: no geodata found for {}'.format(address))
            return False
        return True
    except Exception as exp:
        log.error('Aborting query {}'.format(exp))
        DBSession.rollback()
    finally:
        DBSession.close()