import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# Database connection details
db_params = {
    'dbname': 'Liveprojectdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

# File path to the CSV file (using raw string)
csv_file_path = r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\sim_rtds_prescription.csv'

# Read the CSV file into a pandas DataFrame with specified data types
dtype = {
    'PATIENTID': int,
    'PRESCRIPTIONID': int,
    'RTTREATMENTMODALITY': str,
    'RTPRESCRIBEDDOSE': float,
    'RTPRESCRIBEDFRACTIONS': float,
    'RTACTUALDOSE': float,
    'RTACTUALFRACTIONS': float,
    'RTTREATMENTREGION': str,
    'RTTREATMENTANATOMICALSITE': str,
    'RADIOTHERAPYEPISODEID': int,
    'LINKCODE': str,
    'ATTENDID': str,
    'APPTDATE': str  # Read as string initially to parse date later
}

df = pd.read_csv(csv_file_path, dtype=dtype, parse_dates=['APPTDATE'], dayfirst=True)

# Handle NaN values in APPTDATE column
df['APPTDATE'] = df['APPTDATE'].where(pd.notnull(df['APPTDATE']), None)

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    print("Connected to the database successfully")
except Exception as error:
    print(f"Error connecting to the database: {error}")

# Define the SQL command to create the table if it doesn't exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS sim_rtds_prescription (
    patientid INTEGER,
    prescriptionid INTEGER,
    rttreatmentmodality CHAR(255),
    rtprescribeddose NUMERIC,
    rtprescribedfractions NUMERIC,
    rtactualdose NUMERIC,
    rtactualfractions NUMERIC,
    rttreatmentregion CHAR(255),
    rttreatmentanatomicalsite CHAR(255),
    radiotherapyepisodeid INTEGER,
    linkcode CHAR(255),
    attendid CHAR(255),
    apptdate DATE
);
'''

# Execute the SQL command to create the table
try:
    cursor.execute(create_table_query)
    conn.commit()
    print("Table 'sim_rtds_prescription' created successfully")
except Exception as error:
    print(f"Error creating table: {error}")

# Insert data into the table
insert_query = '''
INSERT INTO sim_rtds_prescription (
    patientid,
    prescriptionid,
    rttreatmentmodality,
    rtprescribeddose,
    rtprescribedfractions,
    rtactualdose,
    rtactualfractions,
    rttreatmentregion,
    rttreatmentanatomicalsite,
    radiotherapyepisodeid,
    linkcode,
    attendid,
    apptdate
) VALUES %s
'''

# Convert the DataFrame to a list of tuples, replacing NaN with None
data = [tuple(x if pd.notnull(x) else None for x in row) for row in df.itertuples(index=False, name=None)]

# Use psycopg2's execute_values to insert the data
try:
    execute_values(cursor, insert_query, data)
    conn.commit()
    print("Data imported successfully")
except Exception as error:
    print(f"Error inserting data: {error}")

# Close the database connection
cursor.close()
conn.close()
