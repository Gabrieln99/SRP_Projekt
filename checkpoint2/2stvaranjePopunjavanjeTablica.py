import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import IntegrityError

CSV_FILE_PATH = "D:/fax_/treca_godina/skladistenje_rudarenje/checkpoint2/flights_najbolji_PROCESSED.csv"  ## tu mora ic i80 posto jer u star shemu ide 20

df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print(f"CSV size: {df.shape}") 
print(df.head()) 

Base = declarative_base()

class Airline(Base):
    __tablename__ = 'airline'
    id = Column(Integer, primary_key=True, autoincrement=True)
    carrier = Column(String(3), nullable=False, unique=True)
    airline_name = Column(String(100), nullable=False)

class Aircraft(Base):
    __tablename__ = 'aircraft'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tailnum = Column(String(10), nullable=False, unique=True)
    airline_fk = Column(Integer, ForeignKey('airline.id'))

class Route(Base):
    __tablename__ = 'route'
    id = Column(Integer, primary_key=True, autoincrement=True)
    origin = Column(String(5), nullable=False)
    destination = Column(String(5), nullable=False)
    departure_city = Column(String(50))
    departure_country = Column(String(50))
    departure_airport_name = Column(String(100))
    destination_city = Column(String(50))
    destination_country = Column(String(50))
    destination_airport_name = Column(String(100))
    distance = Column(Float)

class Dep_delay(Base):
    __tablename__ = 'dep_delay'
    id = Column(Integer, primary_key=True, autoincrement=True)
    reason_dep_delay = Column(String(50))
    dep_delay_time = Column(Float) 

class Arr_delay(Base):
    __tablename__ = 'arr_delay'
    id = Column(Integer, primary_key=True, autoincrement=True)
    reason_arr_delay = Column(String(50))
    arr_delay_time = Column(Float) 

class Flight(Base):
    __tablename__ = 'flight'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    day_of_week = Column(String(10), nullable=False)
    dep_time = Column(String(10))
    arr_time = Column(String(10))
    sched_dep_time = Column(String(10), nullable=False)
    sched_arr_time = Column(String(10), nullable=False)
    hour = Column(Integer)
    minute = Column(Integer)
    air_time = Column(Float)
    flight_num = Column(Integer) 
    dep_delay_fk = Column(Integer, ForeignKey('dep_delay.id'))
    arr_delay_fk = Column(Integer, ForeignKey('arr_delay.id'))
    route_fk = Column(Integer, ForeignKey('route.id'))
    aircraft_fk = Column(Integer, ForeignKey('aircraft.id')) 

engine = create_engine("mysql+pymysql://root:root@localhost/flights_db")

Base.metadata.drop_all(engine)  
Base.metadata.create_all(engine) 

Session = sessionmaker(bind=engine) 
session = Session() 

# Konverzija određenih stupaca u numerički format radi sprječavanja grešaka
df['distance'] = pd.to_numeric(df['distance'], errors='coerce')
df['air_time'] = pd.to_numeric(df['air_time'], errors='coerce')
df['dep_delay_time'] = pd.to_numeric(df['dep_delay_time'], errors='coerce')
df['arr_delay_time'] = pd.to_numeric(df['arr_delay_time'], errors='coerce')

# Umetanje podataka u tablicu 'airline'
airlines = df[['carrier', 'airline_name']].drop_duplicates()  
session.bulk_insert_mappings(Airline, airlines.to_dict(orient="records"))  
session.commit()

airline_map = {a.carrier: a.id for a in session.query(Airline).all()}

# Umetanje podataka u tablicu 'aircraft'
aircrafts = df[['tailnum', 'carrier']].drop_duplicates()
aircrafts['airline_fk'] = aircrafts['carrier'].map(airline_map)
aircrafts = aircrafts.drop(columns=['carrier'])
aircrafts = aircrafts.drop_duplicates(subset=['tailnum'])
session.bulk_insert_mappings(Aircraft, aircrafts.to_dict(orient="records"))
session.commit()
# Mapiranje aviona po tailnum-u
aircraft_map = {a.tailnum: a.id for a in session.query(Aircraft).all()}

# Umetanje podataka u tablicu 'route'
# Napomena: CSV možda nema sve stupce koje želimo za rutu, popunjavamo što imamo
route_columns = ['origin', 'destination', 'distance']
additional_columns = ['departure_city', 'departure_country', 'departure_airport_name', 
                      'destination_city', 'destination_country', 'destination_airport_name']

# Provjera koji stupci postoje u CSV-u
available_columns = []
for col in route_columns:
    if col in df.columns:
        available_columns.append(col)

# Priprema ruta s dostupnim stupcima
routes = df[available_columns].drop_duplicates()

# Dodajemo stupce koji nedostaju s None vrijednostima
for col in additional_columns:
    if col not in df.columns:
        routes[col] = None
    else:
        routes[col] = df[col]

session.bulk_insert_mappings(Route, routes.to_dict(orient="records"))
session.commit()

# Mapiranje ruta
route_map = {(r.origin, r.destination): r.id for r in session.query(Route).all()}

# Umetanje podataka u tablicu 'dep_delay'
dep_delays = df[['reason_dep_delay', 'dep_delay_time']].drop_duplicates()
session.bulk_insert_mappings(Dep_delay, dep_delays.to_dict(orient="records"))
session.commit()

dep_delay_map = {(d.reason_dep_delay, d.dep_delay_time): d.id for d in session.query(Dep_delay).all()}

# Umetanje podataka u tablicu 'arr_delay'
arr_delays = df[['reason_arr_delay', 'arr_delay_time']].drop_duplicates()
session.bulk_insert_mappings(Arr_delay, arr_delays.to_dict(orient="records"))
session.commit()

arr_delay_map = {(a.reason_arr_delay, a.arr_delay_time): a.id for a in session.query(Arr_delay).all()}

# Priprema podataka za umetanje u tablicu 'flight'
flight_columns = ['year', 'month', 'day', 'day_of_week', 'dep_time', 'arr_time', 'sched_dep_time', 
                  'sched_arr_time', 'hour', 'minute', 'air_time', 'flight_num', 
                  'reason_dep_delay', 'dep_delay_time', 'reason_arr_delay', 'arr_delay_time',
                  'origin', 'destination', 'tailnum']

# Provjera koji stupci postoje u CSV-u
available_flight_columns = []
for col in flight_columns:
    if col in df.columns:
        available_flight_columns.append(col)

flights = df[available_flight_columns].copy()

# Postavljanje vrijednosti za foreign ključeve
flights['dep_delay_fk'] = flights.apply(lambda x: dep_delay_map.get((x['reason_dep_delay'], x['dep_delay_time'])), axis=1)
flights['arr_delay_fk'] = flights.apply(lambda x: arr_delay_map.get((x['reason_arr_delay'], x['arr_delay_time'])), axis=1)
flights['route_fk'] = flights.apply(lambda x: route_map.get((x['origin'], x['destination'])), axis=1)
flights['aircraft_fk'] = flights['tailnum'].map(aircraft_map)

# Uklanjanje stupaca koji su sada u tablicama povezani preko foreign key-eva
drop_columns = ['reason_dep_delay', 'dep_delay_time', 'reason_arr_delay', 'arr_delay_time', 
                'origin', 'destination', 'tailnum']
for col in drop_columns:
    if col in flights.columns:
        flights = flights.drop(columns=[col])

# Umetanje podataka u tablicu 'flight'
session.bulk_insert_mappings(Flight, flights.to_dict(orient="records"))
session.commit()

print("Data imported successfully!")