import psycopg2
from psycopg2 import sql

# Database connection details
db_config = {
    'dbname': 'Liveprojectdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

# SQL statement to create table
create_table_query = """
CREATE TABLE IF NOT EXISTS diagnosis (
    Id SERIAL PRIMARY KEY,
    Code VARCHAR(10) NOT NULL,
    CodeWithSeparator VARCHAR(10) NOT NULL,
    ShortDescription VARCHAR(255) NOT NULL,
    LongDescription TEXT NOT NULL,
    HippaCovered BOOLEAN NOT NULL,
    Deleted BOOLEAN NOT NULL
);
"""

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    # Create a cursor object using the connection
    cursor = conn.cursor()

    # Execute the create table query
    cursor.execute(create_table_query)
    # Commit the transaction
    conn.commit()
    print("Table 'diagnosis' created successfully!")

except psycopg2.DatabaseError as e:
    print(f"Error creating table: {e}")

finally:
    # Close communication with the database
    if conn is not None:
        conn.close()
