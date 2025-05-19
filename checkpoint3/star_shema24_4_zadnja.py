from sqlalchemy import create_engine, Column, Integer, Float, String, BigInteger, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/star_shema"
engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

# ----------------------
# DIMENZIJE
# ----------------------

class DimAirline(Base):
    __tablename__ = 'dim_airline'
    __table_args__ = {'schema': 'star_shema'}
    airline_tk = Column(BigInteger, primary_key=True)
    carrier = Column(String(3), nullable=False)
    airline_name = Column(String(100), nullable=False)

class DimAircraft(Base):
    __tablename__ = 'dim_aircraft'
    __table_args__ = {'schema': 'star_shema'}
    aircraft_tk = Column(BigInteger, primary_key=True)
    tailnum = Column(String(10), nullable=False)

class DimRoute(Base):
    __tablename__ = 'dim_route'
    __table_args__ = {'schema': 'star_shema'}
    route_tk = Column(BigInteger, primary_key=True)
    origin = Column(String(5), nullable=False)
    destination = Column(String(5), nullable=False)
    departure_city = Column(String(50))
    departure_country = Column(String(50))
    departure_airport_name = Column(String(100))
    destination_city = Column(String(50))
    destination_country = Column(String(50))
    destination_airport_name = Column(String(100))
    distance = Column(Float)

class DimDepDelay(Base):
    __tablename__ = 'dim_dep_delay'
    __table_args__ = {'schema': 'star_shema'}
    dep_delay_tk = Column(BigInteger, primary_key=True)
    reason = Column(String(50))
    delay_time = Column(Float)

class DimArrDelay(Base):
    __tablename__ = 'dim_arr_delay'
    __table_args__ = {'schema': 'star_shema'}
    arr_delay_tk = Column(BigInteger, primary_key=True)
    reason = Column(String(50))
    delay_time = Column(Float)

class DimDate(Base):
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'star_shema'}
    date_tk = Column(BigInteger, primary_key=True)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    day_of_week = Column(String(10))

class DimTime(Base):
    __tablename__ = 'dim_time'
    __table_args__ = {'schema': 'star_shema'}
    time_tk = Column(BigInteger, primary_key=True)
    dep_time = Column(String(10))
    arr_time = Column(String(10))
    sched_dep_time = Column(String(10))
    sched_arr_time = Column(String(10))
    hour = Column(Integer)
    minute = Column(Integer)

# ----------------------
# ČINJENICA
# ----------------------

class FactFlight(Base):
    __tablename__ = 'fact_flight'
    __table_args__ = {'schema': 'star_shema'}
    flight_tk = Column(BigInteger, primary_key=True)

    airline_id = Column(BigInteger, ForeignKey('star_shema.dim_airline.airline_tk'))
    aircraft_id = Column(BigInteger, ForeignKey('star_shema.dim_aircraft.aircraft_tk'))
    route_id = Column(BigInteger, ForeignKey('star_shema.dim_route.route_tk'))
    dep_delay_id = Column(BigInteger, ForeignKey('star_shema.dim_dep_delay.dep_delay_tk'))
    arr_delay_id = Column(BigInteger, ForeignKey('star_shema.dim_arr_delay.arr_delay_tk'))
    date_id = Column(BigInteger, ForeignKey('star_shema.dim_date.date_tk'))
    time_id = Column(BigInteger, ForeignKey('star_shema.dim_time.time_tk'))

    air_time = Column(Float)
    flight_num = Column(Integer)

# ----------------------
# KREIRANJE TABLICA
# ----------------------

Base.metadata.create_all(engine)
print("Dimenzijski model kreiran u bazi 'star_shema'.")
