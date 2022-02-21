import pandas as pd
from sqlalchemy import create_engine
from etl.Transformation.model import Stg_vehicle_information_schema, Stg_accident_information_schema
from etl.Logger import logger

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
        useless_columns = ["Carriageway_Hazards", 
                        "Did_Police_Officer_Attend_Scene_of_Accident",
                        "Pedestrian_Crossing-Human_Control",
                        "Pedestrian_Crossing-Physical_Facilities",
                        "Special_Conditions_at_Site"]
        accident_infomation = accident_infomation.drop(useless_columns, axis=1)

        # convert data type
        accident_infomation= accident_infomation.dropna()
        accident_infomation["Accident_Index"] = accident_infomation["Accident_Index"].astype("str")
        accident_infomation["1st_Road_Class"] = accident_infomation["1st_Road_Class"].astype("str")
        accident_infomation["1st_Road_Number"] = accident_infomation["1st_Road_Number"].astype("float64")
        accident_infomation["2nd_Road_Class"] = accident_infomation["2nd_Road_Class"].astype("str")
        accident_infomation["2nd_Road_Number"] = accident_infomation["2nd_Road_Number"].astype("float64")
        accident_infomation["Accident_Severity"] = accident_infomation["Accident_Severity"].astype("str")
        accident_infomation["Day_of_Week"] = accident_infomation["Day_of_Week"].astype("str")
        accident_infomation["Junction_Control"] = accident_infomation["Junction_Control"].astype("str")
        accident_infomation["Junction_Detail"] = accident_infomation["Junction_Detail"].astype("str")
        accident_infomation["Police_Force"] = accident_infomation["Police_Force"].astype("str")
        accident_infomation["Road_Surface_Conditions"] = accident_infomation["Road_Surface_Conditions"].astype("str")
        accident_infomation["Road_Type"] = accident_infomation["Road_Type"].astype("str")
        accident_infomation["Speed_limit"] = accident_infomation["Speed_limit"].astype("float64")
        accident_infomation["Urban_or_Rural_Area"] = accident_infomation["Urban_or_Rural_Area"].astype("str")
        accident_infomation["Weather_Conditions"] = accident_infomation["Weather_Conditions"].astype("str")
        accident_infomation["Year"] = accident_infomation["Year"].astype("int")
        accident_infomation["Time"] = accident_infomation["Time"].astype("str")
        accident_infomation["InScotland"] = accident_infomation["InScotland"].astype("bool")
        
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
        
        vehicle_Information["Accident_Index"] = vehicle_Information["Accident_Index"].astype("str")
        vehicle_Information["Age_Band_of_Driver"] = vehicle_Information["Age_Band_of_Driver"].astype("str")
        vehicle_Information["Age_of_Vehicle"] = vehicle_Information["Age_of_Vehicle"].astype("int")
        vehicle_Information["Driver_Home_Area_Type"] = vehicle_Information["Driver_Home_Area_Type"].astype("str")
        vehicle_Information["Engine_Capacity_.CC."] =  vehicle_Information["Engine_Capacity_.CC."].astype("int")
        vehicle_Information["Hit_Object_in_Carriageway"] =  vehicle_Information["Hit_Object_in_Carriageway"].astype("str")
        vehicle_Information["Hit_Object_off_Carriageway"] =  vehicle_Information["Hit_Object_off_Carriageway"].astype("str")
        vehicle_Information["Journey_Purpose_of_Driver"] =  vehicle_Information["Journey_Purpose_of_Driver"].astype("str")
        vehicle_Information["Junction_Location"] = vehicle_Information["Junction_Location"].astype("str")
        vehicle_Information["make"] = vehicle_Information["make"].astype("str")
        vehicle_Information["model"] = vehicle_Information["model"].astype("str")
        vehicle_Information["Propulsion_Code"] = vehicle_Information["Propulsion_Code"].astype("str")
        vehicle_Information["Sex_of_Driver"] = vehicle_Information["Sex_of_Driver"].astype("str")
        vehicle_Information["Skidding_and_Overturning"] = vehicle_Information["Skidding_and_Overturning"].astype("str")
        vehicle_Information["Vehicle_Leaving_Carriageway"] = vehicle_Information["Vehicle_Leaving_Carriageway"].astype("str")
        vehicle_Information["Vehicle_Location.Restricted_Lane"] = vehicle_Information["Vehicle_Location.Restricted_Lane"].astype("int")
        vehicle_Information["Vehicle_Manoeuvre"] = vehicle_Information["Vehicle_Manoeuvre"].astype("str")
        vehicle_Information["Vehicle_Type"] = vehicle_Information["Vehicle_Type"].astype("str")
        vehicle_Information["Was_Vehicle_Left_Hand_Drive"] = vehicle_Information["Was_Vehicle_Left_Hand_Drive"].astype("str")
        vehicle_Information["X1st_Point_of_Impact"] = vehicle_Information["X1st_Point_of_Impact"].astype("str")
        vehicle_Information["Year"] = vehicle_Information["Year"].astype("int")
        
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
        
    
