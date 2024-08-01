import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

# Function to check if table exists
def table_exists(engine, table_name):
    query = text(f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE  table_schema = 'public'
        AND    table_name   = '{table_name}'
    );
    """)
    with engine.connect() as connection:
        result = connection.execute(query)
        return result.scalar()

# Function to drop table if exists
def drop_table_if_exists(engine, table_name):
    if table_exists(engine, table_name):
        query = text(f"DROP TABLE IF EXISTS {table_name};")
        with engine.connect() as connection:
            connection.execute(query)
        print(f"Table '{table_name}' dropped.")

# Function to create and populate lookup tables
def create_and_populate_lookup_table(engine, sheet_name, data):
    table_name = sheet_name.lower().replace(' ', '_').strip()
    
    drop_table_if_exists(engine, table_name)
    
    # Check if the required columns are present
    if 'Code' not in data.columns or 'Description' not in data.columns:
        print(f"Sheet '{sheet_name}' does not contain the required 'Code' and 'Description' columns. Skipping.")
        return

    metadata = MetaData()
    columns = [
        Column('code', String(50), primary_key=True),
        Column('description', String(255))
    ]
    table = Table(table_name, metadata, *columns)

    # Create table
    metadata.create_all(engine)
    print(f"Table '{table_name}' created successfully.")

    # Insert data
    insert_data = [
        {'code': str(row['Code']).replace("'", "''"), 'description': str(row['Description']).replace("'", "''")}
        for index, row in data.iterrows()
    ]
    print(f"Inserting data into table '{table_name}': {insert_data}")  # Debugging statement
    try:
        with engine.begin() as connection:
            connection.execute(table.insert(), insert_data)
        print(f"Data inserted into '{table_name}' successfully.")
    except SQLAlchemyError as e:
        print(f"Error inserting data into table '{table_name}': {e}")

# Load the Excel file
excel_file_path = "C:\\Swinburne\\Internship\\Live Project\\simulacrum_v2.1.0\\simulacrum_v2.1.0\\Documents\\all_z_lookup_tables.xlsx"
lookup_tables = pd.read_excel(excel_file_path, sheet_name=None)

# Database connection details
url = "postgresql://postgres:pass@localhost:5432/live_project"
engine = create_engine(url)

# Create and populate lookup tables
for sheet_name, data in lookup_tables.items():
    if "lookup table description" not in sheet_name.lower():
        create_and_populate_lookup_table(engine, sheet_name, data)
