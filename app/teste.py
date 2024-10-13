from sqlalchemy import inspect,create_engine, Table, Column, Integer,Float, String, MetaData, DateTime
import pandas as pd

engine = create_engine('postgresql://user:password@127.0.0.1:5432/trip_db', pool_pre_ping=True, client_encoding="UTF-8") 

metadata = MetaData() 

#example
sql_query = '''


    SELECT 
        *
    FROM 
        trips
    WHERE 
        origin_coord_latitude BETWEEN 5 AND 40;
        '''
# Load data into DataFrame
tabledata = pd.read_sql_query(sql_query, engine)
print(tabledata)