import psycopg2

# Database connection details
db_config = {
    'dbname': 'Liveprojectdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

# Establish a connection to the database
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

# Alter the columns to have sufficient length
cur.execute("""
    ALTER TABLE sim_av_patient
    ALTER COLUMN gender TYPE VARCHAR(255);
""")
cur.execute("""
    ALTER TABLE sim_av_patient
    ALTER COLUMN vitalstatus TYPE VARCHAR(255);
""")
cur.execute("""
    ALTER TABLE sim_av_patient
    ALTER COLUMN deathlocationcode TYPE VARCHAR(255);
""")
cur.execute("""
    ALTER TABLE sim_av_patient
    ALTER COLUMN ethnicity TYPE VARCHAR(255);
""")

# Update gender column using z_gender lookup table
cur.execute("""
    UPDATE sim_av_patient
    SET gender = z_gender.description
    FROM z_gender
    WHERE sim_av_patient.gender::text = z_gender.code;
""")

# Update vitalstatus column using z_vitalstatus lookup table
cur.execute("""
    UPDATE sim_av_patient
    SET vitalstatus = z_vitalstatus.description
    FROM z_vitalstatus
    WHERE sim_av_patient.vitalstatus = z_vitalstatus.code;
""")

# Update deathlocationcode column using z_deathlocationcode lookup table
cur.execute("""
    UPDATE sim_av_patient
    SET deathlocationcode = z_deathlocationcode.description
    FROM z_deathlocationcode
    WHERE sim_av_patient.deathlocationcode = z_deathlocationcode.code;
""")

# Update ethnicity column using z_ethnicity lookup table
cur.execute("""
    UPDATE sim_av_patient
    SET ethnicity = z_ethnicity.description
    FROM z_ethnicity
    WHERE sim_av_patient.ethnicity = z_ethnicity.code;
""")

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()

print("Database updated successfully.")
