# Stuart Case study
## __How to run pipeline in your computer?__
In order to run pipelines there are some steps needs to be done

1. Set up virtual enviroments (Python >= 3.7)
    ```sh
    cd <path to the project>/stuart_casestudy
    pyenv virtualenv <python-version> <virtualenv-name>
    pyenv activate <virtualenv-name>
    ```
2. Install python dependencies
    ```sh
    cd <path to the project>/stuart_casestudy
    pip install -r requirements.txt
    ```
3. Set the enviroment variable $PATHONPATH 
    ```sh
    export PYTHONPATH=<path to the project>/stuart_casestudy/
    ```
4. Run the docker-compose to have a presudo data warehouse in the backend
   ```sh
   cd <path to the project>/stuart_casestudy
   docker-compose up .
   ```
5. Create an Schema in the data warehouse
   ```sh
   CREATE SCHEMA IF NOT EXISTS kaggle; 
   ```
6. Run the pipline in folder dag/run.py
    ```sh
    cd <path to the project>/stuart_casestudy/dag
    python run.py
    ```

## __What is this ? What are you looking at ?__
The idea is to crack the challenge of modulized the pipeline be able to reuse and reproduce and provide a visualization
There are folders and files in the root directory here I'm going to introduce them one by one
- #### etl 
  - Here include all the actions of pipeline (Extraction, Transformation, Load)
  - The idea is to achived modulization hence need to set up $PATHONPATH include where this etl location
- #### tests
  - The idea here is to add some test cases on methods of etl actions
- #### dags
    - Where all the pipline script
- #### Visualization
    - Here I provided simple data insight with visualization in python notebook
- #### docker-compose.yml
    - Due to the raw data has saved into file system the database is just ephemeral hence I decide to use a docker container so we can have more flexibility in the future.
- #### requirements.txt
   - Python libraries need to be installed before tun the pipeline

> Please be noted that here the stroage solution still using local file for the presentation hence the files from kaggle will be in .etl/Extraction/data/<source_name>/<download time>


## __Why I’ve chosen each tool/language/framework for the task.__
1.  Use Python because there are a lots of packages regards to the data engineering
2. Use Pandas because faster & easier to develpoe
3. Use Pandora because it integrate with Pandas seamleassly


## __Any data quality practices you would enforce as well as error handling__
1. Data consistency 
2. Schema consistency
3.  Able to rewind to the previous version of source dataset
4.  Unit test on some of the actions in ETL


## __How you would test for correctness, i.e. reconcile.__
Here I would have more discussion with the person who have better knowledge on traffic accident to determine the corretness with that said for now I would
Benchmarking the data set by profiling the source data including (I will go with the great_expectation):

- &nbsp; Data row count (more or less 20% or raise error)
- &nbsp; Null data (more or less 10% or raise error)
- &nbsp; Categorical data defination (ex: Animal should include cat, dog, ... but if there is a iphone in the column this should raise an error...)
- &nbsp; Common sense, for example I aware there are drivers with age below 15 which dont make sense so I filter out.


## __How would I really polish the challenge to build this project in production?__
The main idea of this project is to present the concept of how I will handle these tasks doesn't mean I will do this on production. It just a simple project for demostrate purpose.

- &nbsp; For dags I probably will use Airflow because its Python & ability of the scale (K8S or CeleryExecutor)
- &nbsp; For Transformation I will use Snowflake/Redshift + dbt or PySpark (DBT to me is a game changer which I would definitely like to have more hands-on experience in the future)
- &nbsp; For Load & Extraction In general I will use Fivetran or Stitch however in this case with kaggle API I will still use my own scripts
- &nbsp; For the stroage solution I will definately go with S3 furthermore instead of csv files I will convert in parquet file or split into smaller file first and store in S3
- &nbsp; For Visualization I will use Tableau or powerBI, Tableau able to connect to various of sources easily to fetch data in data mart and build the dashboard without the pain.




