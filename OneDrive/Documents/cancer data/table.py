import psycopg2
from psycopg2 import sql

# Database connection parameters
dbname = "Liveprojectdb"
user = "postgres"
password = "123456"
host = "localhost"
port = "5432"

# SQL query to create table
create_table_query = """
CREATE TABLE sim_rtds_combined (
    PATIENTID INTEGER,
    PRESCRIPTIONID INTEGER,
    RTTREATMENTMODALITY VARCHAR(255),
    RADIOTHERAPYPRIORITY VARCHAR(255),
    RADIOTHERAPYINTENT VARCHAR(255),
    RTPRESCRIBEDDOSE NUMERIC,
    RTPRESCRIBEDFRACTIONS INTEGER,
    RTACTUALDOSE NUMERIC,
    RTACTUALFRACTIONS INTEGER,
    RTTREATMENTREGION VARCHAR(255),
    RTTREATMENTANATOMICALSITE VARCHAR(255),
    DECISIONTOTREATDATE DATE,
    EARLIESTCLINAPPROPDATE DATE,
    RADIOTHERAPYEPISODEID INTEGER,
    LINKCODE VARCHAR(255),
    RADIOISOTOPE VARCHAR(255),
    RADIOTHERAPYBEAMTYPE VARCHAR(255),
    RADIOTHERAPYBEAMENERGY NUMERIC,
    TIMEOFEXPOSURE TIME,
    APPTDATE DATE,
    ATTENDID VARCHAR(255)
);
"""

try:
    # Connect to PostgreSQL database
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    print("Connected to database.")

    # Create a cursor object using the connection
    cursor = conn.cursor()

    # Execute the CREATE TABLE query
    cursor.execute(create_table_query)
    conn.commit()

    print("Table sim_rtds_combined created successfully.")

except psycopg2.Error as e:
    print(f"Error creating table: {e}")

finally:
    # Close communication with the database
    if conn:
        cursor.close()
        conn.close()
        print("Connection closed.")
