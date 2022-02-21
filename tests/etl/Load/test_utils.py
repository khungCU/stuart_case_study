import pytest
import pandas as pd
from etl.Load.utils import Load
import os

'''
Need to build the schema before run this test
ex:
CREATE SCHEMA IF NOT EXISTS kaggle_test; 
'''

@pytest.fixture
def load_obj():
    return Load(
        "test_data",
        "localhost",
        "kaggle_test"
    )

def test_load_lastest_version(load_obj):
    
    """
    After reload the lastest versoin the data count should be 3
    (./test_data/200/a.csv has 3 rows of data)
    """
    load_obj.data_path = os.path.join(load_obj.path)
    load_obj.load_lastest_version()
    
    df = pd.read_sql("""
                     SELECT * FROM kaggle_test.source_a;
                     """,
                     con = load_obj.engine)
    assert len(df) == 3
    