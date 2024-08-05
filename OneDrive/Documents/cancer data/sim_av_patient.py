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

# Function to import CSV data into the sim_av_patient table
def import_csv_to_sim_av_patient(csv_file_path):
    # Establish the connection
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Read CSV file into DataFrame
    df = pd.read_csv(csv_file_path)

    # Replace NaN with None
    df = df.where(pd.notnull(df), None)
    
    # Define columns
    columns = [
        'patientid', 'gender', 'ethnicity',
        'deathcausecode_1a', 'deathcausecode_1b', 'deathcausecode_1c',
        'deathcausecode_2', 'deathcausecode_underlying',
        'deathlocationcode', 'vitalstatus', 'vitalstatusdate', 'linknumber'
    ]

    # Prepare the insert query with placeholders
    insert_query = sql.SQL("""
        INSERT INTO sim_av_patient ({fields})
        VALUES ({values})
        ON CONFLICT DO NOTHING
    """).format(
        fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
        values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )

    # Print the query for debugging
    print(insert_query.as_string(conn))
    
    # Insert data row by row
    for row in df.itertuples(index=False, name=None):
        row_data = row[:len(columns)]
        print(row_data)  # Print the row for debugging
        cur.execute(insert_query, row_data)
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

# CSV file path for sim_av_patient
csv_file_path = r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\sim_av_patient.csv'

# Import the CSV file into the sim_av_patient table
import_csv_to_sim_av_patient(csv_file_path)
