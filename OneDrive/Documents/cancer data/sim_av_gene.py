import pandas as pd
from sqlalchemy import create_engine

# Define the PostgreSQL connection URL
url = "postgresql://postgres:123456@localhost:5432/Liveprojectdb"
engine = create_engine(url)

# Define the path to your CSV file
csv_file_path = 'C:\\Users\\anuru\\OneDrive\\Desktop\\simulacrum\\simulacrum_v2.1.0\\Data\\sim_av_gene.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Define the table name
table_name = 'sim_av_gene'

# Write the DataFrame to the PostgreSQL database
df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f"Data from {csv_file_path} has been successfully imported into the {table_name} table.")
