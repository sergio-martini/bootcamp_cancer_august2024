import sqlalchemy
import pandas as pd
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

df = pd.read_csv("C:\Swinburne\Internship\Live Project\simulacrum_v2.1.0\simulacrum_v2.1.0\Data\sim_av_gene.csv")

# Convert DataFrame column names to lowercase
df.columns = [col.lower() for col in df.columns]

url = "postgresql://postgres:pass@localhost:5432/live_project"
engine = create_engine(url)
metadata = MetaData()

columns = [Column(name, infer_sqlalchemy_type(dtype)) for name, dtype in df.dtypes.items()]
table = Table('sim_av_gene', metadata, *columns)
metadata.create_all(engine)  # This will create the table if it does not exist

# Insert data into the table
df.to_sql('sim_av_gene', con=engine, if_exists='append', index=False, chunksize=5000)
