"""
Code Written by Pat Hall
Last updated: 11.21.23
"""
import data_utils as du
import pandas as pd
import geopandas as gpd
import os
import sqlite3 as sql
import psycopg as pg
import re
from dotenv import load_dotenv
import os
from pymongo.mongo_client import MongoClient


"""
SQLITE TOOLS
"""

DB_NAME = 'default.sqlite'
DEFAULT_PATH = os.path.join(os.path.dirname(__file__),DB_NAME)

#sets DEFAULT_PATH to desired path. specifically used for sqlite.
def set_DEFAULT_PATH(db_name = None,db_loc = None):
    global DEFAULT_PATH
    pattern = r'\.(sqlite|db)$'

    if db_name == None:
        print(f"Path is set to: {str(DEFAULT_PATH)}")
        return
    
    else:
        while True:
            if re.search(pattern,db_name) == None:
                print("error: db_name must end with '.sqlite' or .db")
                db_name = input("enter a new name for you Database: ")

            else:
                break

        if db_loc is None:
            DEFAULT_PATH = os.path.join(os.path.dirname(__file__),db_name)

        else:
            DEFAULT_PATH = (db_loc + '/' + db_name)

    print(f"Path is set to: {str(DEFAULT_PATH)}")

#connects to sqlite db
def connect_to_sqlite():
    connection = sql.connect(DEFAULT_PATH)
    return connection

#formats list of dictonaries for insertion into a sqlite db
def prepare_dict(list_of_dicts):
    list_of_tuples = [tuple(value for value in record.values()) for record in list_of_dicts] #syntax for this line is adapted from chatGPT
    return list_of_tuples

"""
POSTGRESQL TOOLS
"""
def insert_rows_gdf(filepath,schema,table_name):
    """
    inserts all rows from a geospatial filetype into a specified postgreSQL table.
    The intention is that a user would run this after "create_table_columns_gdf()". 
    Can handle many different types of geospatial files thanks to geopandas. Take a look at geopandas
    documentation for more specifics.

    Parameters:
    filepath (str): filepath to spatial data file to import. 
    schema (str): name of schema in postgreSQL.
    table_name (str): name of existing table in postgreSQL to import data to.

    Returns:
    Nothing. Self-contained function.
    """
    load_dotenv()
    dbname = os.environ.get('DB_NAME')
    user = os.environ.get("USER")
    password = os.environ.get("PASSWORD")

    try:
        host = os.environ.get("HOST")
        port = os.environ.get("PORT")

    except:
        print("no env variable set for host and/or port.") 
        print("Attempting connection on pgadmin defaults.")
        host = None
        port = None


    if host == None or port == None:
        #establish db connection
        cur,conn = initiate_pg_cursor(dbname,user,password)
        print("connection established")

    else:

        cur,conn = initiate_pg_cursor(dbname,user,password,host,port)
        print("connection established")


    #prep data for insertion to db table
    gdf = gpd.read_file(filepath)

    gdf['geometry']=gdf['geometry'].apply(lambda geom: geom.wkt if geom else None)

    column_names = gdf.columns.tolist()
    corrected_column_names = du.replace_spaces_in_list(column_names)
    gdf.columns = corrected_column_names
    col = ', '.join(list(gdf.columns))
    values = ', '.join(list('%s' for _ in gdf.columns))
    insert_query = f"INSERT INTO {schema}.{table_name} ({col}) VALUES ({values})"

    data_tuples = [tuple(x) for x in gdf.to_numpy()]

  

    print(f'inserting values across {len(corrected_column_names)} columns in {dbname}.{schema}.{table_name}')
    print()
    print('executing INSERT...')
    cur.executemany(insert_query, data_tuples)
    print('INSERT executed.')
    conn.commit()
    print('changes committed!')
    conn.close()
    print('connection closed.')
    print()

    print(f'{len(values)} values have been inserted into "{schema}.{table_name}" in the database {dbname}.')
  
# creates a table in pg from a spatial file type (geojson,shapefile, etc. See geopandas documentation for more.)
def create_table_columns_gdf(filepath,schema,new_table_name):
    """
    Creates a new table in a postgreSQL database from an spatial file. uses geopandas
    to get datatype, then converts to compatible PGSQL datatype with dtype_pd_to_pg().

    Parameters:
    filepath (str): filepath to spatial data file 
    schema (str): name of schema in db to insert table into.
    new_table_name (str): name of new table being created

    Returns:
    Self contained function, does not return any variables.
    """

    load_dotenv()
    dbname = os.environ.get('DB_NAME')
    user = os.environ.get("USER")
    password = os.environ.get("PASSWORD")
    host = os.environ.get("HOST")
    port = os.environ.get("PORT")
    

    cur,conn = initiate_pg_cursor(dbname,user,password,host,port)
    
    gdf = gpd.read_file(filepath)
    column_names = gdf.columns.tolist()
    column_dtypes = gdf.dtypes.tolist()

    pg_dtypes = dtype_pd_to_pg(column_dtypes)
    corrected_column_names = du.replace_spaces_in_list(column_names)

    for colname, coldtype in zip(corrected_column_names,pg_dtypes):
        print(colname,coldtype)

    print()
    print(f'{len(corrected_column_names)} columns identified and formatted.')
    print()

    create_table = f"CREATE TABLE {schema}.{new_table_name} ({','.join([f'{col} {dtype}' for col, dtype in zip(corrected_column_names, pg_dtypes)])})"

    cur.execute(create_table)
    conn.commit()
    conn.close()

    print(f'Table "{schema}.{new_table_name}" has been created in the database {dbname}.')

#Creates a new table using column headers from a csv.
def create_table_columns_from_csv(csv_file_path,schema,new_table_name,delim=';'):

    """
    Creates a new table using column headers from a csv. Does not match data types, all columns will be
    varchar, and will need to be copied ot a new table as their true data type.

    Parameters:
    csv_file_path (str): The path of the csv to create db table from.
    schema (str): name of schema in db to write table to.
    new_table_name (str): name of new table being created in schema.
    delim (str): delimiting character in csv. defaults to ';'.

    Returns:
    nothing. Self-contained function.
    """

    load_dotenv()
    dbname = os.environ.get('DB_NAME')
    user = os.environ.get("USER")
    password = os.environ.get("PASSWORD")
    host = os.environ.get("HOST")
    port = os.environ.get("PORT")

    # Create a connection + cursor to the PostgreSQL database
    cur,conn = initiate_pg_cursor(dbname,user,password,host,port) #connects to db and creates cursor object


    df = pd.read_csv(csv_file_path, sep=delim, nrows=1)  # Read only the first row to get column names
    column_names = df.columns.tolist()
    corrected_column_names = du.replace_spaces_in_list(column_names)

    print(corrected_column_names)
    print(len(corrected_column_names))

    # Generate the SQL table creation statement
    sql_statement = f"CREATE TABLE {schema}.{new_table_name} ({', '.join([f'{col} VARCHAR' for col in corrected_column_names])});"

    # Execute the SQL statement to create the table
    cur.execute(sql_statement)
    conn.commit()
    conn.close()

    print(f'Table "{schema}.{new_table_name}" has been created in the database.')

# creates new pg table from excel worksheet
def create_table_columns_from_excel(xlsx_filepath,ws_name,schema,new_table_name):
    """
    Creates a new table in a postgreSQL database from an excel worksheet. uses pandas
    to get datatype from excel, then converts to compatible PGSQL datatype with 
    dtype_pd_to_pg().

    Parameters:
    xslx_filepath (str): filepath to excel workbook being imported
    ws_name (str): name of work sheet in workbook to reference.
    schema (str): name of schema in db to insert table into.
    new_table_name (str): name of new table being createsd

    Returns:
    Self contained function, does not return any variables.
    """

    load_dotenv()
    dbname = os.environ.get('DB_NAME')
    user = os.environ.get("USER")
    password = os.environ.get("PASSWORD")
    host = os.environ.get("HOST")
    port = os.environ.get("PORT")

    cur,conn = initiate_pg_cursor(dbname,user,password,host,port)
    
    df = pd.read_excel(io=xlsx_filepath,sheet_name=ws_name)
    column_names = df.columns.tolist()
    column_dtypes = df.dtypes.tolist()

    pg_dtypes = dtype_pd_to_pg(column_dtypes)
    corrected_column_names = du.replace_spaces_in_list(column_names)

    for colname, coldtype in zip(corrected_column_names,pg_dtypes):
        print(colname,coldtype)

    print()
    print(f'{len(corrected_column_names)} columns identified and formatted.')
    print()

    create_table = f"CREATE TABLE {schema}.{new_table_name} ({','.join([f'{col} {dtype}' for col, dtype in zip(corrected_column_names, pg_dtypes)])})"

    cur.execute(create_table)
    conn.commit()
    conn.close()

    print(f'Table "{schema}.{new_table_name}" has been created in the database {dbname}.')

# returns a list of datatypes compatible with pg from a list of pandas datatypes
def dtype_pd_to_pg(pd_dtypes):
    """
    Creates a list of postgresql data types equivalent to data types as they currently exist in 
    pandas dataframe. conersions use dictionary, pandas_to_postgresql, in data_utils.py.

    Parameters:
    pd_dtypes (list): list of pandas datatypes to be converted to postgreSQL data types.

    Returns:
    pg_dtypes (list): list of postgreSQL datatypes equivalent to input pandas data types.
    """
    pg_dtypes = []
    for dtype in pd_dtypes:
        pg_dtypes.append(du.pandas_to_postgresql.get(str(dtype),'TEXT'))
    return pg_dtypes

# inserts rows from an excel ws into an already existing pg table.
def insert_rows_from_excel(xlsx_filepath,ws_name,schema,table_name):
    """
    inserts all rows from an excel woeksheet into a specified postgreSQL table.
    The intention is that a user would run this after "create_table_columns_from_excel()".

    Parameters:
    xlsx_filepath (str): filepath to excel workbook being imported
    ws_name (str): name of worksheet within excel workbook to import.
    schema (str): name of schema in postgreSQL.
    table_name (str): name of existing table in postgreSQL to import data to.

    Returns:
    Nothing. Self-contained function.
    """

    load_dotenv()
    dbname = os.environ.get('DB_NAME')
    user = os.environ.get("USER")
    password = os.environ.get("PASSWORD")

    #establish db connection
    cur,conn = initiate_pg_cursor(dbname,user,password)

    #prep data for insertion to db table
    df = pd.read_excel(io=xlsx_filepath,sheet_name=ws_name)
    column_names = df.columns.tolist()
    corrected_column_names = du.replace_spaces_in_list(column_names)
    df.columns = corrected_column_names
    col = ', '.join(list(df.columns))
    values = ', '.join(list('%s' for _ in df.columns))
    insert_query = f"INSERT INTO {schema}.{table_name} ({col}) VALUES ({values})"

    data_tuples = [tuple(x) for x in df.to_numpy()]

  

    print(f'inserting values across {len(corrected_column_names)} columns in {dbname}.{schema}.{table_name}')
    print()
    print('executing INSERT...')
    cur.executemany(insert_query, data_tuples)
    print('INSERT executed.')
    conn.commit()
    print('changes committed!')
    conn.close()
    print('connection closed.')
    print()

    print(f'{len(values)} values have been inserted into "{schema}.{table_name}" in the database {dbname}.')
  
#Initiates a connection and cursor for local postgreSQL session
def initiate_pg_cursor(dbname,user,password,host='localhost',port='5432'):

    """
    Initiates connection and cursor objects for a local postgresql session. most secure way to use
    this function is to pass user and password parameters as environmental variables (see python-dotenv).
    Connects to default port (5432).

    Parameters:
    dbname (str): name of database
    user (str): username. Best if passed from envionmental variable.
    password (str): password. Best if passed from environmental variable.
    host (str): hosting machine string. Best if passed from environmental variable. defaults to 'localhost'
    port (str): port number endpoint. Best if passed from environmental variable. Defaults to '5432', the pg default port. 
    
    Returns:
    cursor (pg cursor object): pass SQL queries to db and retrieve data from db.
    connection (pg connection object): establish and manage connection to db.
    """
   
    try:
        connection = pg.connect(f"dbname={dbname} user={user} password={password} host={host} port={port}")

    except:
        print("error: connection not valid. Try changing connection parameter.")
    

    cursor = connection.cursor()

    return cursor,connection

"""
MONGODB TOOLS
"""

#adapted from MongoDB documentation.
def get_client():
    load_dotenv()

    db_psswd = os.environ.get('MONGO_PASSWORD')
    db_user = os.environ.get('MONGO_USER')
    db_name = os.environ.get('MONGO_NAME')

    uri = f"mongodb+srv://{db_user}:{db_psswd}@{db_name}.rx0xn5a.mongodb.net/?retryWrites=true&w=majority"
    # Create a new client and connect to the server
    client = MongoClient(uri)
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print(f"You successfully connected to {db_name} uri")
        return client
    except Exception as e:
        print(e)