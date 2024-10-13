from datetime import datetime
import time
import io

from sqlalchemy import inspect,create_engine, Table, Column, Integer,Float, String, MetaData, DateTime
import pandas as pd

def create_table(engine,metadata,table_name):
    # The recommended way to check for existence
    if not inspect(engine).has_table("trips"):
        columns =  [
            Column('region', String(100)),
            Column('datetime',DateTime),
            Column('datasource', String(100)),
            Column('origin_coord_latitude', Float),
            Column('destination_coord_latitude',Float),
            Column('origin_coord_longitude', Float),
            Column('destination_coord_longitude', Float),
        ]
        table = Table('trips', metadata, *columns)
        table.create(engine)

def ingest_trips(file_path,job_id):
    # Process in chunks to scale
    # Read CSV file
    print("Agora vai ler a file")
    df = pd.read_csv(file_path)
    print('leu no pd')
    # Get the coordenates as f
    df['origin_coord_latitude'] = df['origin_coord'].apply(lambda x: x.split(' ')[1].replace('(',' ').strip()).astype(float)
    df['destination_coord_latitude'] = df['destination_coord'].apply(lambda x: x.split(' ')[1].replace('(',' ').strip()).astype(float)
    df['origin_coord_longitude'] = df['origin_coord'].apply(lambda x: x.split(' ')[2].replace(')',' ').strip()).astype(float)
    df['destination_coord_longitude'] = df['destination_coord'].apply(lambda x: x.split(' ')[2].replace(')',' ').strip()).astype(float)
    df.drop("origin_coord",inplace=True, axis=1)
    df.drop("destination_coord",inplace=True, axis=1)
    df['datetime'] = pd.to_datetime(df['datetime'])
    # Connect to the database
    engine = create_engine('postgresql://user:password@127.0.0.1:5432/trip_db', pool_pre_ping=True, client_encoding="UTF-8") 
    metadata = MetaData() 
    create_table(engine, metadata,"trips")
    df.to_sql('trips', con=engine, if_exists='append',chunksize=10, index=False) 
