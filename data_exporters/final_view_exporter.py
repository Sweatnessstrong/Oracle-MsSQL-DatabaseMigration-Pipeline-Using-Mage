from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mssql import MSSQL
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def check_text(text):
    '''
    result_text = ""
    for chunk in text.split(' '):
        token = chunk.strip()
        flag_from = 0
        if "," in token:
            for sub_token in token.split(','):
                if '.' in sub_token:
                    if flag_from == 0:
                        result_text += f"{sub_token.split('.')[0].strip()} AS {sub_token.split('.')[1].strip()}"
                else
    '''
    result_text = ""
    
    return result_text
    

def create_view( view_name, query):
    
    view_sql = f"CREATE VIEW fms_csv.{view_name.lower()} AS {query}";
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    try:

        with MSSQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            loader.execute(view_sql)
    except :
        print (view_name)

    return view_sql

@data_exporter
def export_data_to_mssql(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a MSSQL database.
    Specify your configuration settings in 'io_config.yaml'.
    Set the following in your io_config:

    Docs: https://docs.mage.ai/integrations/databases/MicrosoftSQLServer
    """
    
    current_path = path.abspath(path.dirname(__file__))
    base_path = path.join(current_path, "..", "table")
    file_name = "migration_view.txt"
    file_path = path.join(base_path, file_name )

    
    
    with open(file_path, 'r') as read_file:
        flag = 0
        p_index = 1
        for line in read_file:
            row = line.strip()
           
                
            if (row =='{' or row == '}' or row == ''):
                if (flag == 1 ) :
                    
                    if p_index < 4 :
                        continue
                    query = MigQuery.strip().lstrip('"').rstrip('"')
                    final_view_name = view_name.rstrip(',').strip().lstrip('"').rstrip('"')
                    print(f'{p_index}:{final_view_name} ')
                    #create_view(final_view_name.rstrip('"'), query)
                    p_index += 1 
                flag = 0
                
            elif ('index' in row):
                flag = 0
                view_index = row.split(':')[1]
                
            elif ('ViewName' in row):
                flaog = 0
                view_name = row.split(':')[1]
                print (f'view_index : {view_index}  view_name: {view_name}' )
            
            elif ('MigQuery' in row):
                flag = 1 
                MigQuery = row.split(':')[1]+ ' '
            elif (flag == 1 ):
              MigQuery += row+' '
    
    
          
     
            
            
       
    '''
    index = 0
    for  view_name  in df['VIEW_NAME']:
        
        text = df['TEXT'][index]
        #view_sql = view_sql_edit(text, view_name)
        #print('{')
        #print(f'index:{index},')
        print(f'{index }: {view_name}')
        #print(',')
        #print(f'MigQuery:{text}')
        #print('}')
            
        
        
        
        index += 1
    '''
    