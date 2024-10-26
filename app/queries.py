from sqlalchemy import inspect,create_engine, Table, Column, Integer,Float, String, MetaData, DateTime
import pandas as pd

engine = create_engine('postgresql://user:password@db:5432/trip_db', pool_pre_ping=True, client_encoding="UTF-8") 

metadata = MetaData() 

def weekly_average_region(region):
    sql_query = f'''

    SELECT 
        COUNT(*) / 7 AS weekly_average_trips
    FROM 
        trips
    WHERE 
        region = '{region}';

    '''
    tabledata = pd.read_sql_query(sql_query, engine)
    weekly_avg = float(tabledata['weekly_average_trips'].iloc[0])
    return weekly_avg

def weekly_average_lat_long(lat_min, long_min, lat_max, long_max):
    sql_query = f'''

    SELECT 
        COUNT(*) / 7 AS weekly_average_trips
    FROM 
        trips
    WHERE 
        origin_coord_latitude BETWEEN {lat_min} AND {lat_max}
        AND origin_coord_longitude BETWEEN {long_min} AND {long_max};


    '''
    tabledata = pd.read_sql_query(sql_query, engine)
    weekly_avg = float(tabledata['weekly_average_trips'].iloc[0])
    return weekly_avg
