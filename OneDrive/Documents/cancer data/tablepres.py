import psycopg2
from psycopg2 import sql

# Database connection details
db_params = {
    'dbname': 'Liveprojectdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    print("Connected to the database successfully")
except Exception as error:
    print(f"Error connecting to the database: {error}")

# Define the SQL command to create the table
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
    radiotherapyepsiodeid INTEGER,
    linkcode CHAR(255),
    attendid CHAR(255),
    apptdate DATE
);
'''

# Execute the SQL command
try:
    cursor.execute(create_table_query)
    conn.commit()
    print("Table 'sim_rtds_prescription' created successfully")
except Exception as error:
    print(f"Error creating table: {error}")

# Close the database connection
cursor.close()
conn.close()
