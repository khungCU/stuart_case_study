import pytest
import os
from etl.Extraction.utils import Extraction
import shutil
import pandas as pd

@pytest.fixture
def extract_obj():
    return Extraction()

@pytest.fixture
def data_path():
    return os.path.join(os.path.abspath(os.curdir), "data")

@pytest.fixture
def source_keyword():
    return "uk-road-safety-accidents-and-vehicles"


def test_prepare_filesystem(extract_obj, data_path, source_keyword):
    # Delete all the files to clean up the test env 
    if os.path.exists(data_path):
        shutil.rmtree(data_path)
    
    extract_obj.prepare_filesystem(source_keyword)
    
    # After run the methods there should be path set up
    assert "data" in os.listdir(os.curdir) , "folder data did not set up properly"
    assert source_keyword in os.listdir(data_path), f"folder {source_keyword} did not set up properly"
    

def test_download_source_files(extract_obj, data_path, source_keyword):
    # Default result is False
    result = False
    # Delete all the files to clean up the test env
    if os.path.exists(data_path):
        shutil.rmtree(data_path)
    
    extract_obj.prepare_history()
    download_details = extract_obj.download_source_files(source_keyword)
    # Search if csv files has generated
    downloaded_path = os.path.join(data_path, 
                                   download_details["search_keyword"], 
                                   str(download_details["unix_time"]))
    
    for file in os.listdir(downloaded_path):
        if ".csv" in file:
            result = True
        else:
            continue
    assert result, "can not detect the downloaded csv file, please fix it"


def test_update_history(extract_obj):
    """
    After run the method there should be more record(s) in the history.csv file 
    """
    # 1. First remove the history.csv file if exists
    hist_path = os.path.join(os.path.abspath(os.curdir), "history.csv")
    if os.path.isfile(hist_path):
        os.remove(hist_path)
    
    # 2. Recreate the history.csv
    extract_obj.prepare_history()
    
    # 3. At this point the length of history.csv should be 0
    df = pd.read_csv(hist_path)
    assert len(df) == 0
    
    # 4. Once update the history there should be one more record more in the history.csv file
    extract_obj.update_history(
        "TestFile.csv",
        "TestSource",
        12345,
        45678,
        "test/file/path"
    )
    df = pd.read_csv(hist_path)
    assert len(df) == 1
    
    