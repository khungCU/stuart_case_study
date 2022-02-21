import pandas as pd
from sqlalchemy import create_engine
from etl.Transformation.model import Stg_vehicle_information_schema, Stg_accident_information_schema
from etl.Logger import logger
from etl.Transformation import vars

class Transformer:
    def __init__(self, user_name, password, schema_name = "kaggle", target_host="localhost"):
        self.user_name = user_name
        self.password = password
        self.schema_name = schema_name
        self.target_host = target_host
        self.sql_query = f"select * from {self.schema_name}"
        self.engine = create_engine(f'postgresql://{user_name}:{password}@{target_host}:5432/postgres')
    
    def read_source_from_database(self, table_name:str) -> pd.DataFrame:
        logger.info(f"Reading from source table : {table_name}")
        table_name = f".source_{table_name}"
        return pd.read_sql_query(self.sql_query + table_name, con=self.engine)
    
    def write_to_database(self, df:pd.DataFrame ,table_name:str, type="") -> None:
        logger.info(f"Writing to table : {table_name}")
        target_table_name = f"{type}{table_name}"
        target_schema = "kaggle"
        df.to_sql(name = target_table_name,
                  schema = target_schema,
                  index = False,
                  if_exists = "replace",
                  chunksize = 10000,
                  con = self.engine)
        
        
    def stg_accident_information(self):
        table_name = "accident_information"
        accident_infomation = self.read_source_from_database(table_name)

        ### Data Cleaning ###
        logger.info(f"Cleaning table : {table_name}")
        # remove useless columns
        
        accident_infomation = accident_infomation.drop(vars.ACCIDENT_INFORMATION_USELESS_COLUMNS, axis=1)
        accident_infomation = accident_infomation.dropna()

        # convert data type
        for column_name in vars.ACCIDENT_INFORMATION_COLUMNS_WITH_STR_TYPE:
            accident_infomation[column_name] = accident_infomation[column_name].astype("str")
        
        for column_name in vars.ACCIDENT_INFORMATION_COLUMNS_WITH_INT_TYPE:
            accident_infomation[column_name] = accident_infomation[column_name].astype("int")
        
        for column_name in vars.ACCIDENT_INFORMATION_COLUMNS_WITH_BOOL_TYPE:
            accident_infomation[column_name] = accident_infomation[column_name].astype("bool")
        
        
        ## covert column name
        rename_columns = {
            "1st_Road_Class" : "First_Road_Class",
            "1st_Road_Number" : "First_Road_Number",
            "2nd_Road_Class" : "Second_Road_Class",
            "2nd_Road_Number" : "Second_Road_Number"
        }
        accident_infomation = accident_infomation.rename(columns=rename_columns)
        
        ## convert columns to lower case
        rename_columns = {k: k.lower() for k in accident_infomation.columns}
        
        accident_infomation = accident_infomation.rename(columns=rename_columns)

        # Data testing with pandera
        Stg_accident_information_schema.validate(accident_infomation)
        
        ## write back as stg data
        self.write_to_database(accident_infomation, table_name, "stg_")
        
     
    def stg_vehicle_information(self):
        table_name = "vehicle_information"
        vehicle_Information =  self.read_source_from_database(table_name)
        
        
        ### Data Cleaning ###
        logger.info(f"Cleaning table : {table_name}")
        useless_columns = ["Driver_IMD_Decile", "Towing_and_Articulation"]
        vehicle_Information = vehicle_Information.drop(useless_columns, axis=1)
        vehicle_Information = vehicle_Information.dropna()
       
        for column_name in vars.VEHICLE_INFORMATION_COLUMNS_WITH_STR_TYPE:
            vehicle_Information[column_name] = vehicle_Information[column_name].astype("str")
        
        for column_name in vars.VEHICLE_INFORMATION_COLUMNS_WITH_INT_TYPE:
            vehicle_Information[column_name] = vehicle_Information[column_name].astype("int")
        
        ## Filter out the data don't make sense
        ## driver age below 15 doesn't make sense
        vehicle_Information_driver_age = (vehicle_Information["Age_Band_of_Driver"] != "6 - 10") &\
                                        (vehicle_Information["Age_Band_of_Driver"] != "0 - 5") &\
                                        (vehicle_Information["Age_Band_of_Driver"] != "11 - 15")
        vehicle_Information = vehicle_Information[vehicle_Information_driver_age]
        
        
        ## covert column name
        rename_columns = {
            "Engine_Capacity_.CC." : "Engine_Capacity_CC",
            "Vehicle_Location.Restricted_Lane" : "Vehicle_Location_Restricted_Lane"
        }
        vehicle_Information = vehicle_Information.rename(columns=rename_columns)
        
        ## convert columns to lower case
        rename_columns = {k: k.lower() for k in vehicle_Information.columns}
        vehicle_Information = vehicle_Information.rename(columns=rename_columns)
    
        # Data testing with pandera
        Stg_vehicle_information_schema.validate(vehicle_Information)
        
        ## write back as stg data
        self.write_to_database(vehicle_Information, table_name, "stg_")
        
    def merge_vehicle_information_accident_information(self):
        table_name = "vehicle_information_accident_information"
        df_vehicle_Information = self.read_from_database("vehicle_Information")
        df_accident_information = self.read_from_database("accident_information")
        df_merge = df_vehicle_Information.merge(df_accident_information, on="Accident_Index")
        self.write_to_database(df_merge, table_name)
        
    
