from sqlalchemy import inspect,create_engine, Table, Column, Integer,Float, String, MetaData, DateTime
import pandas as pd

engine = create_engine('postgresql://user:password@127.0.0.1:5432/trip_db', pool_pre_ping=True, client_encoding="UTF-8") 

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
    return tabledata

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
    return tabledata
#example
sql_query = '''

select datasource, max(lastests) as latest
from (
    SELECT region, datasource, MAX(datetime) as lastests
    FROM trips
    WHERE region IN (
        SELECT region
        FROM trips
        GROUP BY region
        ORDER BY COUNT(*) DESC
        LIMIT 2
    )
    GROUP BY region, datasource
)
GROUP BY datasource order by latest desc limit 1;
'''
# Load data into DataFrame
