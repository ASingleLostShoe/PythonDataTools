"""
Code Written by Pat Hall
Last Updated: 11.21.23
"""

import json
#opens json file and returns a a parsed python dictionary.

#datatype mappings for converting pandas datatypes to postgresql datatypes.
#used in postgresql section of db_utils.py.
pandas_to_postgresql = {
    'int64': 'BIGINT',            # Large integers
    'int32': 'INTEGER',           # Standard integers
    'uint8': 'SMALLINT',          # Small unsigned integers
    'uint16': 'INTEGER',          # Unsigned small integers (no direct match in PostgreSQL, mapped to INTEGER)
    'uint32': 'BIGINT',           # Unsigned integers (no direct match, mapped to BIGINT)
    'uint64': 'NUMERIC',          # Very large unsigned integers (mapped to NUMERIC in PostgreSQL)
    'float64': 'DOUBLE PRECISION',# Double precision floating point
    'float32': 'REAL',            # Single precision floating point
    'bool': 'BOOLEAN',            # Boolean values (True/False)
    'object': 'TEXT',             # Text or generic objects (assumed to be strings)
    'string': 'VARCHAR',          # String type (mapped to VARCHAR)
    'datetime64[ns]': 'TIMESTAMP',# Timestamps with nanosecond precision
    'timedelta64[ns]': 'INTERVAL',# Time intervals (durations)
    'category': 'TEXT',           # Categorical data, typically stored as TEXT
    'complex64': 'TEXT',          # Complex numbers (no direct match, stored as TEXT)
    'complex128': 'TEXT',         # Complex numbers (no direct match, stored as TEXT)
    'pd.Categorical': 'TEXT',     # Categorical data stored as TEXT
    'pd.StringDtype': 'VARCHAR',  # Pandas string type (mapped to VARCHAR)
    'pd.BooleanDtype': 'BOOLEAN', # Nullable boolean type
    'pd.Int8Dtype': 'SMALLINT',   # Nullable 8-bit integer
    'pd.Int16Dtype': 'SMALLINT',  # Nullable 16-bit integer
    'pd.Int32Dtype': 'INTEGER',   # Nullable 32-bit integer
    'pd.Int64Dtype': 'BIGINT',    # Nullable 64-bit integer
    'pd.Float32Dtype': 'REAL',    # Nullable 32-bit float
    'pd.Float64Dtype': 'DOUBLE PRECISION', # Nullable 64-bit float
    'SparseDtype': 'TEXT',        # Sparse data (no direct match, use TEXT or NUMERIC)
    'Interval': 'INTERVAL',       # Time intervals
    'Period': 'TIMESTAMP',        # Period data (mapped to TIMESTAMP)
    'RASTER': 'RASTER',           # PostGIS Raster type
}


def open_json(fileName):
    with open(fileName, 'r') as fhand:
        parsed_json = json.load(fhand)

    return parsed_json

#Opens .txt file and stores as as a string. 
def read_txt(fileName):
    print("opening " + fileName + "...")
    
    try:
        fhand = open(fileName,'r')
    
    except:
        print("could not locate file " + fileName + " in directory.")
        exit()

    readout = fhand.read()
    sansNewLines = readout.split("\n")

    return sansNewLines

def replace_spaces_in_list(list,char='_'):
    modified_list = [item.replace(" ", char) for item in list]
    return modified_list