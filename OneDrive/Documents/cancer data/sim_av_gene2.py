import sqlalchemy
import pandas as pd
import keyring
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData

def infer_sqlalchemy_type(dtype):
    """ Map pandas dtype to SQLAlchemy's types """
    if "int" in dtype.name:
        return Integer
    elif "float" in dtype.name:
        return Float
    elif "object" in dtype.name:
        return String(500)
    else:
        return String(500)

# Load data from CSV file
df = pd.read_csv('C:\\Users\\anuru\\OneDrive\\Desktop\\simulacrum\\simulacrum_v2.1.0\\Data\\sim_av_gene.csv')

# PostgreSQL connection URL
url = "postgresql://postgres:" + keyring.get_password('postgresql', 'postgres') + '@localhost:5432/Liveprojectdb'

# Create engine and metadata
engine = create_engine(url)
metadata = MetaData()

# Define columns based on dataframe dtypes
columns = [Column(name, infer_sqlalchemy_type(dtype)) for name, dtype in df.dtypes.items()]

# Create a table object
table = Table('sim_av_gene', metadata, *columns)

# Create the table in PostgreSQL (commented out to avoid overwriting if table already exists)
# table.create(engine)

# Import data into PostgreSQL table
df.to_sql('sim_av_gene', con=engine, if_exists='append', index=False, chunksize=5000)

print(f"Data imported successfully into 'sim_av_gene' table.")
