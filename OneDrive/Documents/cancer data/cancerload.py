def generate_sql_lookup_table_corrected(sheet_name, data):
    create_statement = f"CREATE TABLE {sheet_name} (\n    code VARCHAR(50) PRIMARY KEY,\n    description VARCHAR(255)\n);\n"
    insert_statements = [f"INSERT INTO {sheet_name} (code, description) VALUES\n"]
    values = []

    for _, row in data.iterrows():
        # Handle missing or misspelled columns by defaulting to empty strings if not found
        code = str(row.get('Code', '')).replace("'", "''")
        description = str(row.get('Description', row.get('Descripion', ''))).replace("'", "''")  # Fix for 'zgene'
        values.append(f"    ('{code}', '{description}')")

    insert_statement = ",\n".join(values) + ";"
    return create_statement + "\n" + insert_statements[0] + insert_statement

# Regenerate SQL scripts with the correct function
sql_scripts_corrected = {}
for sheet_name, data in lookup_tables.items():
    if "z" in sheet_name.lower() and "lookup table description" not in sheet_name.lower():
        corrected_sheet_name = sheet_name.lower().replace(' ', '_').replace('z', 'lookup_').strip()
        sql_scripts_corrected[corrected_sheet_name] = generate_sql_lookup_table_corrected(corrected_sheet_name, data)

# Print the first few SQL statements as an example
for key, value in sql_scripts_corrected.items():
    print(f"SQL for {key}:\n{value}\n\n")
    break  # Break after the first to not overflow the output