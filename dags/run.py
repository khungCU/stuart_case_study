import sys
import os
from threading import Thread
# in order to make dags be able to run the scripts from other subfolder
main_path_to_append = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
sys.path.append(main_path_to_append)

print(main_path_to_append)

from Extraction.extract import extract
from Load.utils import Load
from Transformation.utils import Transformer 
loader = Load("uk-road-safety-accidents-and-vehicles", "localhost", "kaggle")
transformer = Transformer(user_name = "postgres", password = "postgres")
    
def main():
    # Extract
    extract()

    # Load
    loader.load_lastest_version()

    # Transformation
    Thread(target = transformer.stg_accident_information).start()
    Thread(target = transformer.stg_vehicle_information).start()
    

if __name__ == "__main__":
    main()