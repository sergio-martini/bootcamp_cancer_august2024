import psycopg2

# Database connection parameters
hostname = 'localhost'
database = 'Liveprojectdb'
username = 'postgres'
password = '123456'
port = 5432

# Establishing the connection
conn = psycopg2.connect(
    host=hostname,
    dbname=database,
    user=username,
    password=password,
    port=port
)

# Creating a cursor object
cur = conn.cursor()

# SQL query to create the sim_av_patient table
create_table_query = """
CREATE TABLE sim_av_patient (
    PATIENTID SERIAL PRIMARY KEY,
    GENDER VARCHAR(10),
    ETHNICITY VARCHAR(50),
    DEATHCAUSECODE_1A VARCHAR(20),
    DEATHCAUSECODE_1B VARCHAR(20),
    DEATHCAUSECODE_1C VARCHAR(20),
    DEATHCAUSECODE_2 VARCHAR(20),
    DEATHCAUSECODE_UNDERLYING VARCHAR(20),
    DEATHLOCATIONCODE VARCHAR(20),
    VITALSTATUS VARCHAR(20),
    VITALSTATUSDATE DATE,
    LINKNUMBER VARCHAR(50)
);
"""

# Executing the SQL query
cur.execute(create_table_query)

# Committing the transaction
conn.commit()

# Closing the cursor and connection
cur.close()
conn.close()

print("Table 'sim_av_patient' created successfully")
