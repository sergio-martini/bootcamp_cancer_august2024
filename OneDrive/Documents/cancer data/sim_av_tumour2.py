import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData
import keyring

def infer_sqlalchemy_type(dtype):
    """ Map pandas dtype to SQLAlchemy's types """
    if "int" in str(dtype):
        return Integer
    elif "float" in str(dtype):
        return Float
    elif "object" in str(dtype):
        return String(500)
    else:
        return String(500)

# Read CSV into DataFrame
csv_file = r'C:\Users\anuru\OneDrive\Desktop\simulacrum\simulacrum_v2.1.0\Data\sim_av_tumour.csv'
df = pd.read_csv(csv_file)

# Establish database connection
url = "postgresql://postgres:" + keyring.get_password('postgresql', 'postgres') + '@localhost:5432/cancerdb'
engine = create_engine(url)
metadata = MetaData()

# Define SQLAlchemy Table with DataFrame columns
columns = [Column(name, infer_sqlalchemy_type(dtype)) for name, dtype in df.dtypes.items()]
table = Table('sim_av_tumour', metadata, *columns)

# Create the table in the database (uncomment if table needs to be created)
# table.create(engine)

# Insert DataFrame into PostgreSQL database
df.to_sql('sim_av_tumour', con=engine, if_exists='append', index=False, chunksize=5000)
