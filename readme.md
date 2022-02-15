1. How to run it and how to query the data, explain what we are seeing.
    A. In order to run it you need to docker-compose up from the root directory to have a postgres db in the backend as data warehouse
    B. After build up the db you need to connect to it and set up schema to initialized the enviroment:
        CREATE SCHEMA IF NOT EXISTS kaggle
    C. The Pipeline for this project can be found in "./dag/run.py"
        python  ./dag/run.py
    
    D. I split the project into serveral parts:
        - Extraction 
            # Extract data from kaggle 
            # Record metadata in order to trace back history
        - Loading
            # Load data into warehouse
        - Transformation
            # Data cleaning
            # Data testing
        - Visualization
            # Present as jupyter notebooks

2. Why you’ve chosen each tool/language/framework for the task.
    A. Use Python because there are a lots of packages regards to the data engineering
    B. Use Pandas because faster & easier to develpoe
    C. Use Pandora because it integrate with Pandas seamleassly

3. Any data quality practices you would enforce as well as error handling
    A. Data consistency 
    B. Schema consistency
    C. Able to rewind to the previous version of source dataset

4. How you would test for correctness, i.e. reconcile.
    A. Since I'm not a domain expert of trafficing at this point I can only test data like:
        Benchmark the data set by profiling the source data including (I will go with the great_expectation):
        - data row count
        - null data
        - Common sense, for example I aware there are drivers with age below 15 which dont make sense so I filter out.

5. How would I really build this project in production?
    Disclaimer: The main idea of this project is to present the concept of how I will handle these tasks doesn't mean I will do this on production. I just a simple project for demostrate purpose.

    For ./dags I probably will use Airflow 
    For ./Transformation I will use Snowflake/Redshift + dbt or PySpark
    For ./Load & ./Extraction In general I will use Fivetran however in this case with kaggle I will still use my own scripts
        But the stroage solution I will definately go with S3 furthermore instead of csv files I will convert in parquet file or split into smaller file first and store in S3
    For ./Visualization I will use Tableau or powerBI





