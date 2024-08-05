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

# Update ethnicity column using z_ethnicity lookup table
try:
    cur.execute("""
        UPDATE sim_av_patient
        SET ethnicity = z_ethnicity.description
        FROM z_ethnicity
        WHERE sim_av_patient.ethnicity = z_ethnicity.code;
    """)
    conn.commit()
    print("Ethnicity column updated successfully.")
except psycopg2.DatabaseError as e:
    conn.rollback()
    print(f"Error updating ethnicity column: {e}")

# Close the cursor and connection
cur.close()
conn.close()
