import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData
import keyring  # Ensure keyring is installed and configured for your PostgreSQL password retrieval

# Function to infer SQLAlchemy types based on Pandas data types
def infer_sqlalchemy_type(dtype):
    if "int" in dtype.name:
        return Integer
    elif "float" in dtype.name:
        return Float
    elif "object" in dtype.name:
        return String(500)  # Adjust the length as per your data
    else:
        return String(500)

# Read CSV file into a DataFrame
csv_file = r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\sim_av_tumour.csv'
df = pd.read_csv(csv_file, low_memory=False)  # Set low_memory=False if necessary

# Define PostgreSQL database connection parameters
db_config = {
    'dbname': 'Liveprojectdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

# Construct the database URL
url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

# Create SQLAlchemy engine and metadata
engine = create_engine(url)
metadata = MetaData()

# Define the table name and columns based on DataFrame schema
table_name = 'sim_av_tumour'
columns = [Column(name, infer_sqlalchemy_type(dtype)) for name, dtype in df.dtypes.items()]

# Create or reflect the table structure in PostgreSQL
table = Table(table_name, metadata, *columns, extend_existing=True)
metadata.create_all(engine)  # Create the table if it doesn't exist

# Insert data into PostgreSQL table
try:
    df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=5000)
    print("Data inserted successfully.")
except Exception as e:
    print(f"Error inserting data: {str(e)}")
