from mage_ai.io.mssql import MSSQL
from mage_ai.io.config import ConfigFileLoader
from os import path
import pandas as pd

# Assuming ddf is your DataFrame containing the schema information
# Example ddf DataFrame
# ddf = pd.DataFrame([
#     {'COLUMN_NAME': 'id', 'DATA_TYPE': 'INT'},
#     {'COLUMN_NAME': 'name', 'DATA_TYPE': 'VARCHAR(100)'},
#     {'COLUMN_NAME': 'age', 'DATA_TYPE': 'INT'}
# ])

# Function to generate CREATE TABLE SQL statement from schema DataFrame
def generate_create_table_sql(table_name, schema_df):
    columns_sql = ', '.join([f"{row['COLUMN_NAME']} {row['DATA_TYPE']}" for index, row in schema_df.iterrows()])
    create_table_sql = f"CREATE TABLE {table_name} ({columns_sql});"
    return create_table_sql

# Example usage
table_name = 'your_table_name_here'
create_table_sql = generate_create_table_sql(table_name, ddf)
print(create_table_sql)

# Connect to MSSQL and execute CREATE TABLE statement
config_path = path.join(get_repo_path(), 'io_config.yaml')
config_profile = 'default'

with MSSQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
    try:
        loader.execute(create_table_sql)
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Failed to create table {table_name}: {e}")










from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mssql import MSSQL
from pandas import DataFrame
from os import path
import pandas as pd

# Corrected match_type function
def match_type(data_type):
    match_array = {
        'NUMBER': 'integer',
        'VARCHAR2': 'varchar(255)',  # Adjusted length to a typical default
        'DATE': 'datetime',
        'CHAR': 'nchar(255)',  # Adjusted length to a typical default
    }
    return match_array.get(data_type.upper(), 'varchar(255)')  # Default type if not matched

@data_exporter
def export_data_to_mssql(df: DataFrame, **kwargs) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    failed_table_list = []

    for table_id, (table_name, df_group) in enumerate(df.groupby('table_name'), start=1):
        print(f'No: {table_id} Table Name: {table_name}\n')
        
        header_table = [(row['COLUMN_NAME'], match_type(row['DATA_TYPE'])) for index, row in df_group.iterrows()]
        
        ddf = pd.DataFrame(header_table, columns=['COLUMN_NAME', 'DATA_TYPE'])
        create_table_sql = generate_create_table_sql(table_name, ddf)
        print(create_table_sql)
        
        with MSSQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            try:
                loader.execute(create_table_sql)
                print(f"Table {table_name} created successfully.")
            except Exception as e:
                print(f"Failed to create table {table_name}: {e}")
                continue  # Skip to the next table if creation fails
            
            file_name = table_name.upper()
            base_path = path.join(path.abspath(path.dirname(__file__)), "..", "db-data")
            file_path = path.join(base_path, f"{file_name}.txt")
            
            try:
                with open(file_path, 'r') as file:
                    lines = file.readlines()
                
                for line in lines:
                    values = [value.strip().replace("'", "''") for value in line.strip().split('|')]  # Escape single quotes
                    insert_query = f"INSERT INTO sqldb.fms_csv.{table_name} VALUES ('" + "','".join(values) + "');"
                    
                    loader.execute(insert_query)
            except Exception as e:
                print(f"Failed to insert values into Table:{table_name} - {str(e)}\n")
                failed_table_list.append(table_name)
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mssql import MSSQL
from pandas import DataFrame
from os import path
import pandas as pd

# Assuming 'data_exporter' decorator is defined elsewhere in your code
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def generate_create_table_sql(table_name, schema_df):
    columns_sql = ', '.join([f"[{row['COLUMN_NAME']}] {row['DATA_TYPE']}" for index, row in schema_df.iterrows()])
    create_table_sql = f"CREATE TABLE [sqldb].[fms_csv].[{table_name}] ({columns_sql});"
    return create_table_sql

def match_type(data_type):
    match_array = {
        'NUMBER': 'INTEGER',
        'VARCHAR2': 'VARCHAR(255)',  # Adjust length as needed
        'DATE': 'DATETIME',
        'CHAR': 'NCHAR(255)',  # Adjust length as needed
    }
    return match_array.get(data_type.upper(), 'VARCHAR(255)')  # Default to VARCHAR(255) if no match

@data_exporter
def export_data_to_mssql(df: DataFrame, **kwargs) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    failed_table_list = []
    table_id = 1

    for table_name, df_group in df.groupby('table_name'):
        print(f'No: {table_id} Table Name: {table_name}\n')
        table_id += 1

        header_table = []
        for index, row in df_group.iterrows():
            COLUMN_NAME = row['COLUMN_NAME']
            DATA_TYPE = match_type(row['DATA_TYPE'])
            header_table.append((COLUMN_NAME, DATA_TYPE))

        ddf = pd.DataFrame(header_table, columns=['COLUMN_NAME', 'DATA_TYPE'])
        
        create_table_sql = generate_create_table_sql(table_name, ddf)
        print(create_table_sql)

        with MSSQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            try:
                loader.execute(create_table_sql)
                print(f"Table {table_name} created successfully.")
            except Exception as e:
                print(f"Failed to create table {table_name}: {e}")
                failed_table_list.append(table_name)
                continue  # Skip to the next table if creation fails

            file_name = table_name.upper()
            base_path = path.join(path.abspath(path.dirname(__file__)), "..", "db-data")
            file_path = path.join(base_path, f"{file_name}.txt")
            
            try:
                with open(file_path, 'r') as file:
                    lines = file.readlines()

                for line in lines:
                    values = [value.strip().replace("'", "''") for value in line.strip().split('|')]  # Escape single quotes
                    insert_query = f"INSERT INTO [sqldb].[fms_csv].[{table_name}] VALUES ('" + "','".join(values) + "');"
                    
                    loader.execute(insert_query)
            except Exception as e:
                print(f"Failed to insert values into Table:{table_name} - {str(e)}\n")
                failed_table_list.append(table_name)

# Example DataFrame structure for testing
# df = pd.DataFrame({
#     'table_name': ['MyTable'],
#     'COLUMN_NAME': ['id', 'name'],
#     'DATA_TYPE': ['NUMBER', 'VARCHAR2']
# })

# Assuming the DataFrame 'df' is already populated with the necessary data
# export_data_to_mssql(df)
with OracleDB.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
    try:
        table_contents = loader.load(f"""
            SELECT column_name, data_type 
            FROM all_tab_columns 
            WHERE table_name = '{table_name}'
            ORDER BY column_id
        """)
        
        df = pd.DataFrame(table_contents)
        if table_name == 'FMS_PA_HEADER':
            print(df)
        mssql_table_name = table_name.replace(' ', '_').lower()

        df['table_name'] = mssql_table_name  # Add a new column to store the table name
        dfs.append(df)
    except Exception as e:
        # Handle the exception as per your requirements
        print(f"Failed to load table: {str(e)}")
        failed_table_list.append(table_name)  # Store the failed table
