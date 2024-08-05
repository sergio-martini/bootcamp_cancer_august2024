import pandas as pd
import psycopg2
from psycopg2 import sql

# Database connection parameters
db_params = {
    'dbname': 'Liveprojectdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

# Path to the CSV file
csv_file_path = r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\sim_rtds_combined.csv'

# Define data types for each column
dtype_dict = {
    'patientid': 'int64',
    'prescriptionid': 'int64',
    'rttreatmentmodality': 'str',
    'radiotherapypriority': 'str',
    'radiotherapyintent': 'str',
    'rtprescribeddose': 'float64',
    'rtprescribedfractions': 'float64',
    'rtactualdose': 'float64',
    'rtactualfractions': 'float64',
    'rttreatmentregion': 'str',
    'rttreatmentanatomicalsite': 'str',
    'decisiontotreatdate': 'str',  # Load as string to handle invalid dates
    'earliestclinappropdate': 'str',  # Load as string to handle invalid dates
    'radiotherapyeepisodeid': 'int64',
    'linkcode': 'str',
    'radioisotope': 'str',
    'radiotherapybeamtype': 'str',
    'radiotherapybeamenergy': 'float64',
    'timeofexposure': 'str',
    'apptdate': 'str',  # Load as string to handle invalid dates
    'attendid': 'str'
}

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path, dtype=dtype_dict, low_memory=False)

# Convert column names to lowercase
df.columns = df.columns.str.lower()

# Replace invalid date values with None
df.replace({"1899-": None, "NaN": None}, inplace=True)

# Convert columns to datetime, errors='coerce' will handle invalid parsing as NaT
date_columns = ['decisiontotreatdate', 'earliestclinappropdate', 'apptdate']
for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Create table query
create_table_query = """
CREATE TABLE IF NOT EXISTS sim_rtds_combined (
    patientid INTEGER,
    prescriptionid INTEGER,
    rttreatmentmodality VARCHAR(255),
    radiotherapypriority VARCHAR(255),
    radiotherapyintent VARCHAR(255),
    rtprescribeddose NUMERIC,
    rtprescribedfractions NUMERIC,
    rtactualdose NUMERIC,
    rtactualfractions NUMERIC,
    rttreatmentregion VARCHAR(255),
    rttreatmentanatomicalsite VARCHAR(255),
    decisiontotreatdate DATE,
    earliestclinappropdate DATE,
    radiotherapyeepisodeid INTEGER,
    linkcode VARCHAR(255),
    radioisotope VARCHAR(255),
    radiotherapybeamtype VARCHAR(255),
    radiotherapybeamenergy NUMERIC,
    timeofexposure TIME,
    apptdate DATE,
    attendid VARCHAR(255)
);
"""

# Execute create table query
cur.execute(create_table_query)
conn.commit()

# Insert data into the table
for index, row in df.iterrows():
    insert_query = sql.SQL("""
        INSERT INTO sim_rtds_combined (patientid, prescriptionid, rttreatmentmodality, radiotherapypriority,
            radiotherapyintent, rtprescribeddose, rtprescribedfractions, rtactualdose, rtactualfractions,
            rttreatmentregion, rttreatmentanatomicalsite, decisiontotreatdate, earliestclinappropdate,
            radiotherapyepisodeid, linkcode, radioisotope, radiotherapybeamtype, radiotherapybeamenergy,
            timeofexposure, apptdate, attendid)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    cur.execute(insert_query, [None if pd.isna(x) else x for x in row.tolist()])

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Data imported successfully!")
