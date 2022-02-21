import kaggle
import os
import pandas as pd
import datetime
import time
import zipfile
from etl.Logger import logger
from etl.Extraction import vars

class Extraction:
    
    extraction_main_path = os.path.abspath(os.curdir)
    
    def prepare_filesystem(self, source_keyword:str):
        if "data" not in os.listdir(self.extraction_main_path):
            logger.info("Creating data folder for dataset from kaggle.")
            path = os.path.join(self.extraction_main_path, vars.DEFAULT_DATA_STORE_FOLDER_NAME)
            os.mkdir(path, 0o777)
        else:
            logger.info("data folder already exists continue...")
        
        
        if source_keyword not in os.listdir(os.path.join(self.extraction_main_path, vars.DEFAULT_DATA_STORE_FOLDER_NAME)):
            logger.info(f"Creating {source_keyword} folder for dataset from kaggle.")
            path = os.path.join(self.extraction_main_path, "data", source_keyword)
            os.mkdir(path, 0o777)
        else:
            logger.info(f"{source_keyword} folder already exists continue...")
            
    
    def prepare_history(self):
        # Checking if there is any history.csv file
        # if there is none then see this as a first time hence intialized it
        if vars.DEFAULT_HISTORY_FILE_NAME not in os.listdir(self.extraction_main_path):
            logger.info(f"Creating metadata file {vars.DEFAULT_HISTORY_FILE_NAME}")
            with open(os.path.join(self.extraction_main_path, vars.DEFAULT_HISTORY_FILE_NAME), "w") as file:
                # init header
                file.write(vars.DEFAULT_HISTORY_HEADER)
        else:
            logger.info("History file already exists continue...")
    
    def download_source_files(
    self,
    search_keyword = "uk-road-safety-accidents-and-vehicles"
    ):
        try:
            # initiate the path
            self.prepare_filesystem(search_keyword)
            
            # search the data source
            # ongoing : checking if there is a files to download
            datasets = kaggle.api.dataset_list(search=search_keyword)
            if len(datasets) == 0:
                raise Exception("dataset not found")
            ds = datasets[0]
            
            # List files to downlaod on data source
            file_result = kaggle.api.dataset_list_files(ds.ref)
            files_name = file_result.files
            
            # prepare the file path 
            current_time = datetime.datetime.now()
            unix_time = int(time.mktime(current_time.timetuple()))
            file_path = os.path.join(self.extraction_main_path, vars.DEFAULT_DATA_STORE_FOLDER_NAME, search_keyword)
            # Download the data sources
            # it seems dataset_download_files can't store downloaded file as we want
            # hence here looks a bit weird
            kaggle.api.dataset_download_files(ds.ref, path=file_path)
            
            # unzip the data
            zip_file = [file for file in os.listdir(file_path) if vars.ZIP_EXTENSION in file][0]
            zip_file = os.path.join(file_path ,zip_file)
            csv_file_path = os.path.join(file_path, str(unix_time))
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(csv_file_path)
            # remove zip file
            os.remove(zip_file)
            
            for file in files_name:
                file_name = file.name
                logger.info(file_name)
                source_name = search_keyword
                file_size = os.path.getsize(os.path.join(csv_file_path, file_name))
                download_time = unix_time
                file_path = os.path.join(csv_file_path, file_name)
                self.update_history(file_name, 
                            source_name, 
                            file_size, 
                            download_time,
                            file_path)
            return {
                "search_keyword": search_keyword,
                "file_path": file_path,
                "unix_time": unix_time,
                "files_name": files_name
            }
        except Exception as e:
            logger.error("Kaggle api error")
            logger.error(e)
    
    def update_history(self, file_name:str, source_name:str ,filesize:int, download_time:int, file_path:str):
        try:
            # update info first, the flag info
            df = pd.read_csv(os.path.join(self.extraction_main_path, vars.DEFAULT_HISTORY_FILE_NAME))
            logger.info(file_name)
            logger.info(source_name)
            cond = (df.file_name == file_name) & (df.source_name == source_name)
            df.loc[cond,'is_lastest'] = False
            df.to_csv(os.path.join(self.extraction_main_path, vars.DEFAULT_HISTORY_FILE_NAME), index=False)
            
            # Add new record
            df = pd.read_csv(os.path.join(self.extraction_main_path, vars.DEFAULT_HISTORY_FILE_NAME))
            update_time = datetime.datetime.utcfromtimestamp(download_time).strftime('%Y-%m-%d %H:%M:%S')
            new_record = {"downlaod_time": update_time, 
                        "file_name": file_name, 
                        "source_name" :source_name,
                        "file_size": filesize, 
                        "is_lastest": "True",
                        "file_path": file_path}
            
            new_df = pd.DataFrame(new_record, index=[0])
            
            df = pd.concat([df, new_df])
            df.to_csv(os.path.join(self.extraction_main_path, vars.DEFAULT_HISTORY_FILE_NAME), index=False)
            
        except Exception as e:
            logger.error("update history error")
            logger.error(e)
    
    def extract_uk_road_safety_accidents_and_vehicle(self):
        self.prepare_history()
        self.download_source_files()