import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, MetaData, ForeignKey, Integer
from sqlalchemy.exc import SQLAlchemyError

# Load the Excel file containing lookup tables
excel_file_path = r"C:\Swinburne\Internship\Live Project\simulacrum_v2.1.0\simulacrum_v2.1.0\Documents\all_z_lookup_tables.xlsx"
lookup_tables = pd.read_excel(excel_file_path, sheet_name=None)

# Database connection details
url = "postgresql://postgres:pass@localhost:5432/live_project"
engine = create_engine(url)
metadata = MetaData()

# Explicit list of lookup table names to match the provided names
lookup_table_names = [
    'z_administration_route',
    'z_behaviour',
    'z_cancercareplanintent',
    'z_comorbidities',
    'z_deathlocationcode',
    'z_ethnicity',
    'z_gender_',
    'z_gene',
    'z_grade',
    'z_intent_of_treatment',
    'z_laterality',
    'z_perf_status_start_of_cycle',
    'z_performancestatus',
    'z_radiotherapybeamtype',
    'z_radiotherapyintent',
    'z_radiotherapypriority',
    'z_regimen_outcome_summary',
    'z_rttreatmentmodality',
    'z_stage',
    'z_treatmentregion',
    'z_vitalstatus'
]

# Function to create and populate lookup tables
def create_and_populate_lookup_table(engine, sheet_name, data, table_name):
    # Check if the required columns are present
    if 'Code' not in data.columns or 'Description' not in data.columns:
        print(f"Sheet '{sheet_name}' does not contain the required 'Code' and 'Description' columns. Skipping.")
        return

    columns = [
        Column('code', String(50), primary_key=True),
        Column('description', String(255))
    ]
    table = Table(table_name, metadata, *columns)
    
    metadata.create_all(engine)
    print(f"Table '{table_name}' created successfully.")

    insert_data = [
        {'code': str(row['Code']).replace("'", "''"), 'description': str(row['Description']).replace("'", "''")}
        for index, row in data.iterrows()
    ]
    try:
        with engine.begin() as connection:
            connection.execute(table.insert(), insert_data)
        print(f"Data inserted into '{table_name}' successfully.")
    except SQLAlchemyError as e:
        print(f"Error inserting data into table '{table_name}': {e}")

# Create and populate lookup tables
for sheet_name, data in lookup_tables.items():
    if sheet_name.lower() in lookup_table_names:
        create_and_populate_lookup_table(engine, sheet_name, data, sheet_name.lower())

# Function to link lookup tables with main tables
def link_lookup_tables(engine):
    # List of relationships between main tables and lookup tables
    relationships = [
        {'main_table': 'sim_av_patient', 'main_column': 'gender', 'lookup_table': 'z_gender_', 'lookup_column': 'code'},
        # Add other relationships here based on your requirements
    ]

    for relation in relationships:
        main_table = relation['main_table']
        main_column = relation['main_column']
        lookup_table = relation['lookup_table']
        lookup_column = relation['lookup_column']

        try:
            alter_query = f"""
            ALTER TABLE {main_table}
            ADD CONSTRAINT fk_{main_table}_{main_column}
            FOREIGN KEY ({main_column}) REFERENCES {lookup_table}({lookup_column});
            """
            with engine.connect() as connection:
                connection.execute(alter_query)
            print(f"Foreign key constraint added between {main_table}.{main_column} and {lookup_table}.{lookup_column}")
        except SQLAlchemyError as e:
            print(f"Error adding foreign key constraint: {e}")

# Link lookup tables with main tables
link_lookup_tables(engine)
