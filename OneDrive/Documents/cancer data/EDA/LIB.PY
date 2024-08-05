import pandas as pd
from sqlalchemy import create_engine

# Define your database parameters
db_params = {
    'dbname': 'Liveprojectdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

# Create an SQLAlchemy engine
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

# Load data into DataFrame
query = "SELECT * FROM sim_av_patient"
df = pd.read_sql(query, engine)
# Display the first few rows of the DataFrame
df.head()

# Display the data types and non-null counts
df.info()

# Summary statistics for numerical columns
df.describe()
# Count missing values for each column
missing_values = df.isnull().sum()
missing_values = missing_values[missing_values > 0]
print("Missing values in each column:\n", missing_values)
