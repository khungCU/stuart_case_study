import os
import pandas as pd
from sqlalchemy import create_engine
import logging

#Creating and Configuring logging
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s")
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("LoadLog")

path = os.path.dirname(os.path.abspath(__file__))
path = os.path.split(path)[0]
data_path = os.path.join(path, "Extraction", "data")

'''
Need to build the schema before run this scripts
ex:
CREATE SCHEMA IF NOT EXISTS kaggle; 
'''

class Load:
    def __init__(
        self,
        source_name,
        target_host,
        target_schema    
        ):
        self.user_name = "postgres"
        self.password = "postgres"
        self.source_name = source_name
        self.target_host = target_host
        self.target_schema = target_schema
        
        self.engine = create_engine(f'postgresql://{self.user_name}:{self.password}@{self.target_host}:5432/postgres')
    
    def load_lastest_version(self):
        
        path_to_load = os.path.join(data_path, self.source_name)
        lastest_folder_to_load = max(os.listdir(path_to_load))
        file_path_to_load = os.path.join(path_to_load, lastest_folder_to_load)
        
        for file in os.listdir(file_path_to_load):
            if '.csv' in file:
                df = pd.read_csv(os.path.join(file_path_to_load, file), low_memory=False, encoding="cp850")
                table_name = file.lower().replace(".csv","")
                logger.info(f"Loading {table_name} into database")
                df.to_sql(
                    name = f"source_{table_name}",
                    schema = self.target_schema,
                    index = False,
                    if_exists = "replace",
                    chunksize = 10000,
                    con = self.engine
                )
                logger.info(f"{table_name} loaded !!")