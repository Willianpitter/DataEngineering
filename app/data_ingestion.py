from datetime import datetime
import time
import io

from sqlalchemy import inspect,create_engine, Table, Column, Integer,Float, String, MetaData, DateTime
import pandas as pd

def create_table(engine,metadata,table_name):
    """
    Create the trips table if it doesn't already exist.
    
    Args:
        engine: SQLAlchemy engine object.
        metadata: SQLAlchemy metadata object.
        table_name: Name of the table to create.
    """

    if not inspect(engine).has_table(table_name):
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
        table.create(engine) # Creates the table if it doesn't exist


def ingest_trips(file_path,job_id):
        
    """
    Ingest trip data from a CSV file and store it in the database and extract latitude and longitude from a coordinate column.
    
    Args:
        file_path: Path to the CSV file.
        job_id: Job identifier (for logging/tracking purposes).
    """        
        
    # Process in chunks to handle big files
    # Read CSV file
    df = pd.read_csv(file_path)
    
    # extract latitude and longitude from a coordinate column
    df['origin_coord_latitude'] = df['origin_coord'].apply(lambda x: x.split(' ')[1].replace('(',' ').strip()).astype(float)
    df['destination_coord_latitude'] = df['destination_coord'].apply(lambda x: x.split(' ')[1].replace('(',' ').strip()).astype(float)
    df['origin_coord_longitude'] = df['origin_coord'].apply(lambda x: x.split(' ')[2].replace(')',' ').strip()).astype(float)
    df['destination_coord_longitude'] = df['destination_coord'].apply(lambda x: x.split(' ')[2].replace(')',' ').strip()).astype(float)
    df.drop("origin_coord",inplace=True, axis=1)
    df.drop("destination_coord",inplace=True, axis=1)
    df['datetime'] = pd.to_datetime(df['datetime'])

    if df['datetime'].isnull().any():
        print("Warning: Some datetime values could not be parsed.")

   # Connect to the PostgreSQL database
    try:
        engine = create_engine('postgresql://user:password@127.0.0.1:5432/trip_db', pool_pre_ping=True, client_encoding="UTF-8")
        metadata = MetaData()
        create_table(engine, metadata, "trips")
    except Exception as e:
        print(f"Database connection error: {e}")
        return

    # Ingest the DataFrame into the 'trips' table in the database
    try:
        df.to_sql('trips', con=engine, if_exists='append', chunksize=1000, index=False)
        print(f"Data successfully ingested for job_id {job_id}.")
    except Exception as e:
        print(f"Error during data ingestion: {e}")            