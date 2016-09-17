import csv
import logging
import os
import sys
import time
import models
from netaddr import IPNetwork
from dbmodels import Location, IPV4_Network, IPV6_Network
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import relationship

Base = declarative_base()

DBSession = scoped_session(sessionmaker())

# lambda for converting strings to UTF-8 encoding
utf8_encode = lambda x: unicode(x, 'utf-8', 'ignore')

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')
log = logging.getLogger('setup_sql')

def ingest_ip_addresses():
    with open('GeoLite2-City-Blocks-IPv4.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)  # skip the headers
        log.info('loaded IPV4 network data')
        t0 = time.time()
        num = 0
        for row in reader:
            cidr = row[0]
            record = IPV4_Network(**{
                'cidr': cidr,
                'network_address': int(IPNetwork(cidr)[0]),
                'broadcast_address': int(IPNetwork(cidr)[-1]),
                'geoname_id': row[1],
                'registered_country_geoname_id': row[2],
                'represented_country_geoname_id': row[3],
                'is_anonymous_proxy': row[4],
                'is_satellite_provider': row[5],
                'postal_code': row[6],
                'latitude': row[7],
                'longitude': row[8],
                'accuracy_radius': row[9]
            })
            DBSession.add(record)
            num += 1
            log.debug('indexed row {}'.format(num))
            if num % 1000 == 0:
                DBSession.flush() # periodically flush the pending records
                log.info('flush @ {} secs'.format(str(time.time() - t0)))
        log.info('commit @ {} secs'.format(str(time.time() - t0)))
        DBSession.commit() # commit all records

def ingest_city_locations():
    with open('GeoLite2-City-Locations-en.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)  # skip the headers
        log.info('loaded city location data')
        t0 = time.time()
        num = 0
        for row in reader:
            cidr = row[0]
            record = Location(**{
                'geoname_id':               int(row[0]),
                'locale_code':              utf8_encode(row[1]),
                'continent_code':           utf8_encode(row[2]),
                'continent_name':           utf8_encode(row[3]),
                'country_iso_code':         utf8_encode(row[4]),
                'country_name':             utf8_encode(row[5]),
                'subdivision_1_iso_code':   utf8_encode(row[6]),
                'subdivision_1_name':       utf8_encode(row[7]),
                'subdivision_2_iso_code':   utf8_encode(row[8]),
                'subdivision_2_name':       utf8_encode(row[9]),
                'city_name':                utf8_encode(row[10]),
                'metro_code':               utf8_encode(row[11]),
                'time_zone':                utf8_encode(row[12]),
            })
            DBSession.add(record)
            num += 1
            log.debug('indexed row {}'.format(num))
            if num % 1000 == 0:
                DBSession.flush() # periodically flush the pending records
                log.info('flush @ {} secs'.format(str(time.time() - t0)))
        log.info('commit @ {} secs'.format(str(time.time() - t0)))
        DBSession.commit() # commit all records

def main():
    # creates a sqlite file if there isn't one
    engine = create_engine('sqlite:///{db_name}.sqlite'.format(db_name="geoip"),
                            encoding='utf8',
                            echo='debug')

    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)
    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # http://docs.sqlalchemy.org/en/latest/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow
    try:
        ingest_ip_addresses()
        ingest_city_locations()
    except Exception as exp:
        log.error('Rolling back ingestion {}'.format(exp))
        DBSession.rollback()
    finally:
        DBSession.close()
    log.info('done ingesting data')

if __name__ == '__main__':
    main()
