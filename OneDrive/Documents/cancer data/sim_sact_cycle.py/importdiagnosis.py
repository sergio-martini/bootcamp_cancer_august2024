import psycopg2
import csv

# Database connection details
db_config = {
    'dbname': 'Liveprojectdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

# Path to your CSV file
csv_file_path = r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\diagnosis.csv'

# SQL statement to insert data into the diagnosis table
insert_query = """
INSERT INTO diagnosis (Id, Code, CodeWithSeparator, ShortDescription, LongDescription, HippaCovered, Deleted)
VALUES (%s, %s, %s, %s, %s, %s::BOOLEAN, %s::BOOLEAN);
"""

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    # Create a cursor object using the connection
    cursor = conn.cursor()

    # Open the CSV file
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row

        # Iterate over each row in the CSV file
        for row in csv_reader:
            # Check if the row has exactly 7 elements (matching the number of columns in the INSERT statement)
            if len(row) == 7:
                # Execute the insert query with each row's data, ensuring data types match
                cursor.execute(insert_query, (
                    int(row[0]),    # Id (INT)
                    row[1],         # Code (VARCHAR(10))
                    row[2],         # CodeWithSeparator (VARCHAR(10))
                    row[3],         # ShortDescription (VARCHAR(255))
                    row[4],         # LongDescription (VARCHAR(255))
                    row[5] == '1',  # HippaCovered (BOOLEAN)
                    row[6] == '1'   # Deleted (BOOLEAN)
                ))
                print(f"Inserted row: {row}")  # Debug statement to confirm successful insertion
            else:
                print(f"Skipping row: {row}. Expected 7 columns, got {len(row)} columns.")

    # Commit the transaction
    conn.commit()
    print("Data imported successfully!")

except psycopg2.Error as e:
    print(f"Error importing data: {e}")
    conn.rollback()  # Rollback the transaction in case of error

finally:
    # Close communication with the database
    if conn is not None:
        cursor.close()
        conn.close()
