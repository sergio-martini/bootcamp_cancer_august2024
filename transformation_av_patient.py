import psycopg2

# Database connection parameters
host = "localhost"
port = "5432"
dbname = "live_project"
user = "postgres"
password = "pass"

# SQL query to drop the existing view
drop_view_query = "DROP VIEW IF EXISTS sim_av_patient_transformed;"

# SQL query to create the view with type casting
create_view_query = """
CREATE OR REPLACE VIEW sim_av_patient_transformed AS
SELECT 
    p.PATIENTID::INTEGER,
    g.description AS GENDER,  -- Transform GENDER to its description
    e.description AS ETHNICITY,  -- Transform ETHNICITY to its description
    d1.shortdescription AS DEATHCAUSECODE_1A,  -- Transform DEATHCAUSECODE_1A to its description
    d2.shortdescription AS DEATHCAUSECODE_1B,  -- Transform DEATHCAUSECODE_1B to its description
    d3.shortdescription AS DEATHCAUSECODE_1C,  -- Transform DEATHCAUSECODE_1C to its description
    d4.shortdescription AS DEATHCAUSECODE_2,  -- Transform DEATHCAUSECODE_2 to its description
    d5.shortdescription AS DEATHCAUSECODE_UNDERLYING,  -- Transform DEATHCAUSECODE_UNDERLYING to its description
    dl.description AS DEATHLOCATIONCODE,  -- Transform DEATHLOCATIONCODE to its description
    vs.description AS VITALSTATUS,  -- Transform VITALSTATUS to its description
    p.VITALSTATUSDATE::DATE,
    p.LINKNUMBER::INTEGER
FROM 
    sim_av_patient p
LEFT JOIN 
    z_gender_ g ON p.GENDER::VARCHAR = g.code  -- Cast p.GENDER to VARCHAR for join
LEFT JOIN 
    z_ethnicity e ON p.ETHNICITY::VARCHAR = e.code  -- Cast p.ETHNICITY to VARCHAR for join
LEFT JOIN 
    diagnosis d1 ON p.DEATHCAUSECODE_1A::VARCHAR = d1.code  -- Cast to VARCHAR for join
LEFT JOIN 
    diagnosis d2 ON p.DEATHCAUSECODE_1B::VARCHAR = d2.code  -- Cast to VARCHAR for join
LEFT JOIN 
    diagnosis d3 ON p.DEATHCAUSECODE_1C::VARCHAR = d3.code  -- Cast to VARCHAR for join
LEFT JOIN 
    diagnosis d4 ON p.DEATHCAUSECODE_2::VARCHAR = d4.code  -- Cast to VARCHAR for join
LEFT JOIN 
    diagnosis d5 ON p.DEATHCAUSECODE_UNDERLYING::VARCHAR = d5.code  -- Cast to VARCHAR for join
LEFT JOIN 
    z_deathlocationcode dl ON p.DEATHLOCATIONCODE::VARCHAR = dl.code  -- Cast to VARCHAR for join
LEFT JOIN 
    z_vitalstatus vs ON p.VITALSTATUS::VARCHAR = vs.code;  -- Cast to VARCHAR for join
"""

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=password
)

# Create a cursor object
cur = conn.cursor()

# Execute the query to drop the existing view
cur.execute(drop_view_query)

# Execute the query to create the view
cur.execute(create_view_query)

# Commit the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("View created or replaced successfully.")
