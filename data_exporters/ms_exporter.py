from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mssql import MSSQL
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_mssql(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a MSSQL database.
    Specify your configuration settings in 'io_config.yaml'.
    Set the following in your io_config:

    Docs: https://docs.mage.ai/integrations/databases/MicrosoftSQLServer
    """
    
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    failed_table_list = []
    with MSSQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        table_id = 1
        for table_name, df_group in df.groupby('table_name'):
            print(f'No: {table_id}  Table Name: {table_name}\n')
            try:
                loader.export(
                    df_group,
                    None,
                    table_name,
                    index=False,  # Specifies whether to include index in exported table
                    if_exists='replace',  # Specify resolution policy if table already exists
                )
            except Exception as e:
                # Handle the exception as per your requirements
                print(f"Failed to export Table:{table_name} - {str(e)}\n")
                failed_table_list.append(table_name)

            table_id = table_id + 1
    # Print the table_name that were not successfully exported
    if failed_table_list:
        print("Table that were not added:")
        for table_name in failed_table_list:
            print(table_name)
    else:
        print("All tables were successfully exported.")

    
        