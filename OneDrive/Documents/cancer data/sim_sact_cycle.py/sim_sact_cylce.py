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

# Function to import CSV data into the sim_sact_cycle table
def import_csv_to_sim_sact_cycle(csv_file_path):
    # Read CSV file into DataFrame
    df = pd.read_csv(csv_file_path)

    # Replace NaN with None
    df = df.replace({np.nan: None})
    
    # Columns for sim_sact_cycle table
    columns = ['merged_regimen_id', 'merged_cycle_id', 'cycle_number', 'start_date_of_cycle', 'opcs_procurement_code', 'perf_status_start_of_cycle']

    # Prepare the insert query with placeholders
    insert_query = sql.SQL("""
        INSERT INTO {table} ({fields})
        VALUES ({values})
        ON CONFLICT DO NOTHING
    """).format(
        table=sql.Identifier('sim_sact_cycle'),
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

# Path to the CSV file for sim_sact_cycle
csv_file_path = r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\sim_sact_cycle.csv'

# Import the CSV file into the sim_sact_cycle table
import_csv_to_sim_sact_cycle(csv_file_path)

# Close the cursor and connection
cur.close()
conn.close()
