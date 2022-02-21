from threading import Thread

from etl.Extraction.utils import Extraction
from etl.Load.utils import Load
from etl.Transformation.utils import Transformer
import os

import vars

## In case others run this script in relative path
os.chdir(os.path.abspath(__file__))

loader = Load(vars.SOURCE_KEYWORD, vars.WAREHOUSE_HOST, vars.WAREHOUSE_SCHEMA)
transformer = Transformer(user_name = vars.WAREHOUSE_USERNAME, password = vars.WAREHOUSE_PASSWORD)
    
def main():
    # Extract
    extract = Extraction()
    extract.extract_uk_road_safety_accidents_and_vehicle()

    # Load
    loader.load_lastest_version()

    # Transformation
    Thread(target = transformer.stg_accident_information).start()
    Thread(target = transformer.stg_vehicle_information).start()
    

if __name__ == "__main__":
    main()