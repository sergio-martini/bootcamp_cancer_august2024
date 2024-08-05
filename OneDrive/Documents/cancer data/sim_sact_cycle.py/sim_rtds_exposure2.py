import psycopg2
import pandas as pd
from psycopg2 import sql
import numpy as np

# Database connection parameters
db_params = {
    'dbname': 'Liveprojectdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'  # default PostgreSQL port
}

# Establish the connection
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Function to import CSV data into tables
def import_csv_to_table(csv_file_path, table_name, columns):
    # Read CSV file into DataFrame with specified dtype
    df = pd.read_csv(csv_file_path, dtype=str)
    
    # Replace NaN with None
    df = df.where(pd.notnull(df), None)
    
    # Convert columns to lowercase for SQL query compatibility
    columns = [col.lower() for col in columns]

    # Prepare the insert query with placeholders
    insert_query = sql.SQL("""
        INSERT INTO {table} ({fields})
        VALUES ({values})
        ON CONFLICT DO NOTHING
    """).format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
        values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )

    # Insert data row by row
    for row in df.itertuples(index=False, name=None):
        # Validate and handle TIMEOFEXPOSURE format
        time_of_exposure = row[columns.index('timeofexposure')]
        if time_of_exposure is None:
            continue
        
        try:
            time_obj = pd.to_datetime(time_of_exposure, format='%H:%M:%S').time()
        except ValueError:
            try:
                time_obj = pd.to_datetime(time_of_exposure, format='%H:%M').time()
            except ValueError:
                # Handle or skip rows with invalid time format
                continue
        
        # Handle '1899-' values as None or default value based on your requirement
        if time_of_exposure.startswith('1899-'):
            time_obj = None
        
        # Replace TIMEOFEXPOSURE with validated time_obj
        row_data = list(row[:len(columns)])
        row_data[columns.index('timeofexposure')] = time_obj
        
        cur.execute(insert_query, row_data)
    
    conn.commit()

# CSV file path and corresponding table name and columns for sim_rtds_exposure
csv_file_path = r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\sim_rtds_exposure.csv'
table_name = 'sim_rtds_exposure'
columns = ['PRESCRIPTIONID', 'RADIOISOTOPE', 'RADIOTHERAPYBEAMTYPE', 'RADIOTHERAPYBEAMENERGY', 'TIMEOFEXPOSURE', 'APPTDATE', 'ATTENDID', 'PATIENTID', 'RADIOTHERAPYEPISODEID', 'LINKCODE']

# Import CSV file into sim_rtds_exposure table
import_csv_to_table(csv_file_path, table_name, columns)

# Close the cursor and connection
cur.close()
conn.close()
