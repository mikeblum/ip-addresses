from sqlalchemy import Column, ForeignKey, Integer, Numeric, Float, Boolean, String
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import relationship

Base = declarative_base()

class Record(object):
    """
    Generic record methods to add to SqlAlchemy.Base
    """
    def as_dict(record):
        '''
        Convert a SQLAlchemy record to a
        Python dict object for easy serialization
        '''
        return {c.name: getattr(record, c.name) for c in record.__table__.columns}

class Location(Record, Base):
    __tablename__ = 'locations'
    geoname_id = Column(Integer, primary_key=True, nullable=False)
    locale_code = Column(String())
    continent_code = Column(String())
    continent_name = Column(String())
    country_iso_code = Column(String())
    country_name = Column(String())
    subdivision_1_iso_code = Column(String())
    subdivision_1_name = Column(String())
    subdivision_2_iso_code = Column(String())
    subdivision_2_name = Column(String())
    city_name = Column(String())
    metro_code = Column(String())
    time_zone = Column(String())

class GeoIP_Network(Record):
    # enable auto-numbering
    __table_args__ = {'sqlite_autoincrement': True}
    cidr = Column(String(), primary_key=True, nullable=False)
    network_address = Column(Integer, nullable=False) # lower bound of available addresses in CIDR
    broadcast_address = Column(Integer, nullable=False) # upper bound
    geoname_id = Column(Integer, nullable=False)
    registered_country_geoname_id = Column(Integer, nullable=False)
    represented_country_geoname_id = Column(Integer, nullable=False),
    is_anonymous_proxy = Column(Boolean),
    is_satellite_provider = Column(Boolean),
    postal_code = Column(String())
    latitude = Column(Float)
    longitude = Column(Float)
    accuracy_radius = Column(Integer)

class IPV4_Network(GeoIP_Network, Base):
    __tablename__ = 'ipv4_networks'

class IPV6_Network(GeoIP_Network, Base):
    __tablename__ = 'ipv6_networks'