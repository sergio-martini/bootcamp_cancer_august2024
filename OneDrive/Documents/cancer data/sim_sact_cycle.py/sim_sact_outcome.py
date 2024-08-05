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
    df = df.replace({np.nan: None})
    
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

# CSV file and corresponding table name and columns for sim_sact_outcome
csv_file = {
    'file': r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\sim_sact_outcome.csv',
    'table': 'sim_sact_outcome',
    'columns': [
        'MERGED_REGIMEN_ID', 'DATE_OF_FINAL_TREATMENT', 'REGIMEN_MOD_DOSE_REDUCTION', 
        'REGIMEN_MOD_TIME_DELAY', 'REGIMEN_MOD_STOPPED_EARLY', 'REGIMEN_OUTCOME_SUMMARY'
    ]
}

# Import the CSV file into the corresponding table
import_csv_to_table(csv_file['file'], csv_file['table'], csv_file['columns'])

# Close the cursor and connection
cur.close()
conn.close()
