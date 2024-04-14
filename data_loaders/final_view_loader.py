from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.oracledb import OracleDB
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_oracledb(*args, **kwargs):
    """
    Template for loading data from a OracleDB database.
    Specify your configuration settings in 'io_config.yaml'.

    """
    owner_name = 'FMS_SVC'
    query = f"SELECT VIEW_NAME , TEXT FROM ALL_VIEWS WHERE OWNER = '{owner_name}'"
    
     # Specify your SQL query here
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    view_list = []
    with OracleDB.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        result = loader.load(query)
        
    
    return result

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined '