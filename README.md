# Data transformations with PySpark

This is a collection of _Python_ jobs that are supposed to transform data.
These jobs are using _PySpark_ to process larger volumes of data and are supposed to run on a _Spark_ cluster (via `spark-submit`).

## Pre-requisites
Please make sure you have the following installed and can run them
* Python (3.9 or later), you can use for example [pyenv](https://github.com/pyenv/pyenv#installation) to manage your python versions locally
* [Poetry](https://python-poetry.org/docs/#installation)
* Java (1.8)

## Install all dependencies
```bash
poetry install
```

## Run tests
To run all tests:
```bash
make tests
```

### Run unit tests
```bash
make unit-test
```

### Run integration tests
```bash
make integration-test
```

## Create package
This will create a `tar.gz` and a `.wheel` in `dist/` folder:
```bash
poetry build
```
More: https://python-poetry.org/docs/cli/#build

## Run style checks
```bash
make style-checks
```
This is running the linter and a type checker.

## Jobs

There are two applications in this repo: Word Count, and Citibike.

Currently, these exist as skeletons, and have some initial test cases

### Word Count
A NLP model is dependent on a specific input file. This job is supposed to preprocess a given text file to produce this
input file for the NLP model (feature engineering). This job will count the occurrences of a word within the given text
file (corpus). 


#### Run the job
Please make sure to package the code before submitting the spark job (`poetry build`)
```bash
poetry run spark-submit \
    --master local \
    --py-files dist/src/data_transformations-*.whl \
    src/jobs/word_count.py \
    <INPUT_FILE_PATH> \
    <OUTPUT_PATH>
```

### Citibike
For analytics purposes the BI department of a bike share company would like to present dashboards, displaying the
distance each bike was driven. There is a `*.csv` file that contains historical data of previous bike rides. This input
file needs to be processed in multiple steps. There is a pipeline running these jobs.


##### Run the job
Please make sure to package the code before submitting the spark job (`poetry build`)
```bash
poetry run spark-submit \
    --master local \
    --py-files dist/src/data_transformations-*.whl \
    src/jobs/citibike_ingest.py \
    <INPUT_FILE_PATH> \
    <OUTPUT_PATH>
```

#### Distance calculation
This job takes bike trip information and calculates the "as the crow flies" distance traveled for each trip.
It reads the previously ingested data parquet files.

Hint:
 - For distance calculation, we use [**Harvesine formula**](https://en.wikipedia.org/wiki/Haversine_formula) formula. 

##### Run the job
Please make sure to package the code before submitting the spark job (`poetry build`)
```bash
poetry run spark-submit \
    --master local \
    --py-files dist/src/data_transformations-*.whl \
    src/jobs/citibike_distance_calculation.py \
    <INPUT_PATH> \
    <OUTPUT_PATH>
```
