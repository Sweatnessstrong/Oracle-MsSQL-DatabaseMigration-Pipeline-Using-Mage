from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.oracledb import OracleDB
from mage_ai.io.mssql import MSSQL
import os
from os import path
import csv
import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



@data_loader
def get_table_names(*args, **kwargs):
    #query = "SELECT OWNER, TABLE_NAME FROM ALL_INDEXES ;"
    # Establish a connection
    
    query = 'SELECT DISTINCT OWNER FROM ALL_INDEXES;'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    owner_name = 'FMS_SVC'
    with OracleDB.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        result = loader.load(f"SELECT DISTINCT TABLE_NAME FROM ALL_TABLES WHERE OWNER = '{owner_name}'")
    table_names = result['TABLE_NAME'].tolist()
    query_template = 'SELECT * FROM {}'
    move_counts = 1
    dfs = []
    failed_table_list = []
    for table_name in table_names:
        # Add your logic or processing for each table here
        print(f"index: {move_counts} Table Name:  {table_name}\n")
        move_counts = move_counts + 1
        if (move_counts > 3 ):
            continue;
        
        with OracleDB.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            try:
                table_contents = loader.load(f"SELECT * FROM {owner_name}.{table_name}")
                df = pd.DataFrame(table_contents)
                if 'EMAIL_BODY' in df.columns:
                    df['EMAIL_BODY'] = df['EMAIL_BODY'].astype(str)
                mssql_table_name = table_name.replace(' ', '_').lower()
            
                df['table_name'] = mssql_table_name  # Add a new column to store the table name
                dfs.append(df)
                
            except Exception as e:
                # Handle the exception as per your requirements
                print(f"Failed to load table: {str(e)}")
                failed_table_list.append(table_name)  # Store the failed table

    # Print the tables that were not successfully loaded
    if failed_table_list:
        print("Table that were not loaded:")
        for table_name in failed_table_list:
            print(table_name)
    else:
        print("All tables were successfully loaded.")
    result_df = pd.concat(dfs).reset_index(drop=True)
    return result_df        
            
    

@test
def test_output(output, *args):
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    # Add more tests based on your requirements