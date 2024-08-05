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

# Create the sim_sact_drug_detail table
cur.execute("""
    CREATE TABLE IF NOT EXISTS sim_sact_drug_detail (
        merged_drug_detail_id INTEGER PRIMARY KEY,
        merged_cycle_id INTEGER,
        actual_dose_per_administration NUMERIC,
        opcs_delivery_code CHARACTER VARYING,
        administration_route CHARACTER VARYING,
        administration_date DATE,
        drug_group CHARACTER VARYING
    )
""")
conn.commit()

# Function to import CSV data into sim_sact_drug_detail table
def import_csv_to_sim_sact_drug_detail(csv_file_path):
    # Read CSV file into DataFrame
    df = pd.read_csv(csv_file_path)

    # Replace NaN with None
    df = df.replace({np.nan: None})
    
    # Define columns in lowercase
    columns = ['merged_drug_detail_id', 'merged_cycle_id', 'actual_dose_per_administration', 'opcs_delivery_code', 'administration_route', 'administration_date', 'drug_group']

    # Prepare the insert query with placeholders
    insert_query = sql.SQL("""
        INSERT INTO sim_sact_drug_detail ({fields})
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

# File path to the CSV file
csv_file_path = r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\sim_sact_drug_detail.csv'

# Import the CSV file into the sim_sact_drug_detail table
import_csv_to_sim_sact_drug_detail(csv_file_path)

# Close the cursor and connection
cur.close()
conn.close()
