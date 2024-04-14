from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mssql import MSSQL
from pandas import DataFrame
import pandas as pd
import re
from datetime import datetime
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def generate_create_table_sql(table_name, schema_df):
    drop_table_sql = f"IF OBJECT_ID('sqldb.fms_csv.{table_name}', 'U') IS NOT NULL DROP TABLE sqldb.fms_csv.{table_name};"
    columns_sql = ', '.join([f"[{row['COLUMN_NAME']}] {row['DATA_TYPE']}" for index, row in schema_df.iterrows()])
    create_table_sql = f"CREATE TABLE sqldb.fms_csv.{table_name} ({columns_sql});"
    return drop_table_sql + " " + create_table_sql

def match_type(data_type):
    match_array = {
        'NUMBER': 'INT',  # Changed from 'INTEGER' to 'INT' for MSSQL compatibility
        'VARCHAR2': 'VARCHAR(255)',
        'DATE': 'DATETIME',
        'CHAR': 'CHAR(255)',  # Adjust length as needed
    }
    return match_array.get(data_type.upper(), 'VARCHAR(255)')  # Default to VARCHAR(255) if no match

# Define a regex pattern for dates in the "DD-MON-YY" format
date_pattern = re.compile(r'\d{2}-[A-Z]{3}-\d{2}')

# Function to check if a string matches the "DD-MON-YY" date format
def matches_date_format(value):
    return bool(date_pattern.match(value))

# Function to convert date from "DD-MON-YY" to "YYYY-MM-DD"
def format_date(date_str):
    try:
        return datetime.strptime(date_str, '%d-%b-%y').strftime('%Y-%m-%d')
    except ValueError:
        # Return None or a default date string if parsing fails
        return None


def format_value_for_sql(value, data_type):
    """
    Formats the value based on the data type for MSSQL.
    """
    
    if data_type == 'DATETIME':
        # Attempt to parse and format the date
        try:
            # Adjust the date format as per your input data's format
            formatted_date = datetime.strptime(value, '%d-%b-%y').strftime('%Y-%m-%d')
            return f"CONVERT(DATETIME, '{formatted_date}', 120)"
        except ValueError:
            # Handle the case where the date conversion fails
            return 'NULL'
    elif data_type.startswith('VARCHAR') or data_type == 'CHAR':
        # For string types, escape single quotes and wrap the value in single quotes
        result = value.replace("\'", "\'\'")
        return f"'{result}'"
    elif data_type == 'INT' or data_type == 'NUMBER':
        # For integer, try converting or default to 0 (or NULL based on your preference)
        try:
            int_value = int(value)
            return str(int_value)
        except ValueError:
            return '0'
    # Add more data type handling as needed
    else:
        # Default case for types not explicitly handled
        return f"'{value}'"

@data_exporter
def export_data_to_mssql(df: DataFrame, **kwargs) -> None:
    current_path = path.abspath(path.dirname(__file__))
    base_path = path.join(current_path, "..", "db-data")
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    for table_name, df_group in df.groupby('table_name'):
        print(f'Table Name: {table_name}\n')
        
        
        file_name = table_name.upper()
        file_path = path.join(base_path, file_name + ".txt")
        
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()
        except Exception as e:
            print(f"Failed to read {file_name}.txt: {e}")
            continue
        
        header_table = [(row['COLUMN_NAME'], match_type(row['DATA_TYPE'])) for index, row in df_group.iterrows()]
        ddf = pd.DataFrame(header_table, columns=['COLUMN_NAME', 'DATA_TYPE'])
        
        create_table_sql = generate_create_table_sql(table_name, ddf)
        print(create_table_sql)
        
        with MSSQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            try:
                loader.execute(create_table_sql)
                print(f"Table {table_name} created successfully.")
                
                for line in lines:
                    values = line.strip().split('|')
                    
                    formatted_values = [format_value_for_sql(value.strip(), data_type) for value, (_, data_type) in zip(values, header_table)]
                    
                    insert_query = f"INSERT INTO sqldb.fms_csv.{table_name} VALUES ({', '.join(formatted_values)})"
                    try:
                        loader.execute(insert_query)
                    except Exception as e:
                        print(f"Failed to insert values into table {table_name}: {e}")

                        
                        
                        
            except Exception as e:
                print(f"Failed to insert values into Table:{table_name} - {str(e)}\n")
                        
            
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def generate_create_table_sql(table_name, schema_df):
    drop_table_sql = f"IF OBJECT_ID('sqldb.fms_csv.{table_name}', 'U') IS NOT NULL DROP TABLE sqldb.fms_csv.{table_name};"
    columns_sql = ', '.join([f"[{row['COLUMN_NAME']}] {row['DATA_TYPE']}" for index, row in schema_df.iterrows()])
    create_table_sql = f"CREATE TABLE sqldb.fms_csv.{table_name} ({columns_sql});"
    return drop_table_sql + " " + create_table_sql

def match_type(data_type):
    match_array = {
        'NUMBER': 'INT',  # Changed from 'INTEGER' to 'INT' for MSSQL compatibility
        'VARCHAR2': 'VARCHAR(255)',
        'DATE': 'DATETIME',
        'CHAR': 'CHAR(255)', 
        'TIMESTAMP(6)': 'DATETIME2' # Adjust length as needed
    }
    return match_array.get(data_type.upper(), 'VARCHAR(255)')  # Default to VARCHAR(255) if no match

# Define a regex pattern for dates in the "DD-MON-YY" format
date_pattern = re.compile(r'\d{2}-[A-Z]{3}-\d{2}')

# Function to check if a string matches the "DD-MON-YY" date format
def matches_date_format(value):
    return bool(date_pattern.match(value))

# Function to convert date from "DD-MON-YY" to "YYYY-MM-DD"
def format_date(date_str):
    try:
        return datetime.strptime(date_str, '%d-%b-%y').strftime('%Y-%m-%d')
    except ValueError:
        # Return None or a default date string if parsing fails
        return None


def format_value_for_sql(value, data_type):
    """
    Formats the value based on the data type for MSSQL.
    """
    
    if data_type == 'DATETIME':
        # Attempt to parse and format the date
        try:
            # Adjust the date format as per your input data's format
            formatted_date = datetime.strptime(value, '%d-%b-%y').strftime('%Y-%m-%d')
            return f"CONVERT(DATETIME, '{formatted_date}', 120)"
        except ValueError:
            # Handle the case where the date conversion fails
            return 'NULL'
    elif  data_type == 'DATETIME2':
        # Attempt to parse and format the date
        try:
            # The provided format '19-MAY-23 12.00.00.000000 AM' matches this pattern
            formatted_date = datetime.strptime(value, '%d-%b-%y %I.%M.%S.%f %p')
            # For SQL Server DATETIME2, converting to a string that SQL Server can understand
            # Note: SQL Server DATETIME2 format is 'YYYY-MM-DD HH:MM:SS.nnnnnnn'
            sqlserver_formatted_date = formatted_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Truncate to 3 decimal places for fractional seconds
            return f"CONVERT(DATETIME2, '{sqlserver_formatted_date}')"
        except ValueError as e:
            # Handle the case where the date conversion fails
            print(f"Error converting timestamp: {e}")
            return 'NULL'
    elif data_type.startswith('VARCHAR') or data_type == 'CHAR':
        # For string types, escape single quotes and wrap the value in single quotes
        result = value.replace("\'", "\'\'")
        return f"'{result}'"
    elif data_type == 'INT' or data_type == 'NUMBER':
        # For integer, try converting or default to 0 (or NULL based on your preference)
        try:
            int_value = int(value)
            return str(int_value)
        except ValueError:
            return '0'
    # Add more data type handling as needed
    else:
        # Default case for types not explicitly handled
        return f"'{value}'"
