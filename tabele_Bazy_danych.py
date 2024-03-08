from sqlalchemy import create_engine, Column, Integer, String, Float, MetaData, Table
from sqlalchemy.orm import sessionmaker
import csv


engine = create_engine('sqlite:///:memory:')


metadata = MetaData()


stations_table = Table('stations', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('station', String),
                       Column('latitude', Float),
                       Column('longitude', Float),
                       Column('elevation', Float),
                       Column('name', String),
                       Column('country', String),
                       Column('state', String)
                       )


measure_table = Table('measure', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('station', String),
                      Column('date', String),
                      Column('precip', Float),
                      Column('tobs', Integer)
                      )


metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


with open('clean_stations.csv', 'r') as file:
    stations_data = list(csv.DictReader(file))
    for row in stations_data:
        session.execute(stations_table.insert().values(row))

with open('clean_measure.csv', 'r') as file:
    measure_data = list(csv.DictReader(file))
    for row in measure_data:
        session.execute(measure_table.insert().values(row))


result = session.execute("SELECT * FROM stations LIMIT 5").fetchall()
print(result)
