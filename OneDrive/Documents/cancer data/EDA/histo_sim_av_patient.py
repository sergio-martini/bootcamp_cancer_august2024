import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# Database connection parameters
db_params = {
    'dbname': 'Liveprojectdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'  # default PostgreSQL port
}

# Create a connection string
connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"

# Create a database engine
engine = create_engine(connection_string)

# Query data from the database
query = "SELECT * FROM sim_av_patient"
df = pd.read_sql(query, engine)

# Plot histograms for numerical features
df.hist(figsize=(15, 10), bins=30, edgecolor='k')
plt.show()

# Box plots to identify outliers
plt.figure(figsize=(15, 10))
numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
for i, column in enumerate(numeric_columns, 1):
    plt.subplot(len(numeric_columns), 1, i)
    sns.boxplot(y=df[column])
    plt.title(f'Box plot of {column}')
plt.tight_layout()
plt.show()
