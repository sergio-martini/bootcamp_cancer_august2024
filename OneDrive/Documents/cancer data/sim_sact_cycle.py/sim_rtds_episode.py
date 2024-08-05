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
    # Read CSV file into DataFrame
    df = pd.read_csv(csv_file_path)

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

    # Print the query for debugging
    print(insert_query.as_string(conn))
    
    # Insert data row by row
    for row in df.itertuples(index=False, name=None):
        row_data = row[:len(columns)]
        print(row_data)  # Print the row for debugging
        cur.execute(insert_query, row_data)
    conn.commit()

# Path to the CSV file
csv_file_path = r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\sim_rtds_episode.csv'

# Columns for sim_rtds_episode table
columns = [
    'PATIENTID', 'RADIOTHERAPYEPISODEID', 'ATTENDID', 'APPTDATE', 'LINKCODE',
    'DECISIONTOTREATDATE', 'EARLIESTCLINAPPROPDATE', 'RADIOTHERAPYPRIORITY', 'RADIOTHERAPYINTENT'
]

# Import CSV data into sim_rtds_episode table
import_csv_to_table(csv_file_path, 'sim_rtds_episode', columns)

# Close the cursor and connection
cur.close()
conn.close()
