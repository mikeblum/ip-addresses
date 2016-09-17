#!/usr/bin/env python

"""
Migration engone for denormalizing the IPv4 CIDRs and their associated
metata and joing each CIDR with its associated geographic metadata for
consolidated indexing
"""

import logging
from dbmodels import Location, IPV4_Network, IPV6_Network
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

    try:
        # yield_per helps with joining the three million+ rows of CIDRs and their metadata
        query = DBSession.query(IPV4_Network, Location).\
                    join(Location, IPV4_Network.geoname_id == Location.geoname_id).\
                    yield_per(100)
        for result in query:
            for segment in result:
                pp.pprint(as_dict(segment))
    except Exception as exp:
        log.error('Aborting query {}'.format(exp))
        DBSession.rollback()
    finally:
        DBSession.close()

if __name__ == '__main__':
    main()