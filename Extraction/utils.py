import json
import logging
import kaggle
import os
import pandas as pd
import datetime
import time
import zipfile

#Creating and Configuring logging
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s")
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("ExtractLog")

extraction_main_path = os.path.dirname(os.path.abspath(__file__))

def prepare_history():
    # Checking if there is any history.csv file
    # if there is none then see this as a first time hence intialized it
    if "history.csv" not in os.listdir(extraction_main_path):
        with open(os.path.join(extraction_main_path, "history.csv"), "w") as file:
            # init header
            file.write('"downlaod_time","file_name","source_name","file_size","is_lastest","file_path"')
    else:
        logger.info("History file already exists continue...")
        
def download_source_files(
    search_keyword = "uk-road-safety-accidents-and-vehicles"
):
    try:
        with open(os.path.join(extraction_main_path, "kaggle.json"), "rb") as json_file:
            api_key = json.loads(json_file.readline())
            user_name = api_key["username"]
    except Exception as e:
        logger.error("read key error")
        logging.error(e)
    
    try:
        api = kaggle.api
        api.get_config_value(user_name)
        
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
        file_path = os.path.join(extraction_main_path, "data", search_keyword)
        # Download the data sources
        # it seems dataset_download_files can't store downloaded file as we want
        # hence here looks a bit weird
        kaggle.api.dataset_download_files(ds.ref, path=file_path)
        
        # unzip the data
        zip_file = [file for file in os.listdir(file_path) if ".zip" in file][0]
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
            update_history(file_name, 
                           source_name, 
                           file_size, 
                           download_time,
                           file_path)
    except Exception as e:
        logger.error("Kaggle api error")
        logger.error(e)
        

def update_history(file_name, source_name ,filesize, download_time, file_path):
    try:
        # update info first, the flag info
        df = pd.read_csv(os.path.join(extraction_main_path, "history.csv"))
        logger.info(file_name)
        logger.info(source_name)
        cond = (df.file_name == file_name) & (df.source_name == source_name)
        df.loc[cond,'is_lastest'] = False
        df.to_csv(os.path.join(extraction_main_path, "history.csv"), index=False)
        
        # Add new record
        df = pd.read_csv(os.path.join(extraction_main_path, "history.csv"))
        update_time = datetime.datetime.utcfromtimestamp(download_time).strftime('%Y-%m-%d %H:%M:%S')
        new_record = {"downlaod_time": update_time, 
                    "file_name": file_name, 
                    "source_name" :source_name,
                    "file_size": filesize, 
                    "is_lastest": "True",
                    "file_path": file_path}
        
        new_df = pd.DataFrame(new_record, index=[0])
        
        df = pd.concat([df, new_df])
        df.to_csv(os.path.join(extraction_main_path, "history.csv"), index=False)
        
    except Exception as e:
        logger.error("update history error")
        logger.error(e)
    

if __name__ == "__main__":
    prepare_history()
    download_source_files()