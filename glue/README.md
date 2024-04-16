# Basic Boto3 Glue Crawler Demo
* Cloud Formation Template: Creates S3 bucket and Role for Glue Service
* Boto3:  Creates Glue crawler, database, tables and runs glue job to join json and csv data
* Dervived from https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/python
* Jupyter notebook used for run job dev, from glue dir: jupyter notebook
* https://docs.aws.amazon.com/glue/latest/dg/notebook-getting-started.html



## Clone and install in its own venv
git clone git@github.com:ltm893/aws-big-data.git
Cloning into 'aws-big-data'...
remote: Enumerating objects: 230, done.
remote: Counting objects: 100% (230/230), done.
remote: Compressing objects: 100% (163/163), done.
remote: Total 230 (delta 112), reused 172 (delta 60), pack-reused 0
Receiving objects: 100% (230/230), 85.67 KiB | 1.82 MiB/s, done.
Resolving deltas: 100% (112/112), done.
ltm893@River:~$ cd aws-big-data/
ltm893@River:~/aws-big-data$ python3 -m venv .venv
ltm893@River:~/aws-big-data$ source .venv/bin/activate
(.venv) ltm893@River:~/aws-big-data$ pip install -r requirements.txt
(.venv) ltm893@River:~/aws-big-data$ cd glue/
(.venv) ltm893@River:~/aws-big-data/glue$ python run_glue_demo.py deploy

## Test Data
File: people.json.gz  
Format : 
```javascript
[
  {
    "name": "Lktn Oycjtgukk", #  random strings
    "usage": "933", # random number
    "zip": "57379" # random US zip cocd 
  },
]
```

File: zip_group.csv.gz  
Format: 
|random zip code| random number|
|---|---|
|60629| 3|
|79936| 8|
|11368| 10|
|90650| 1|



## Usage
python run_glue_demo.py --help
usage: run_glue_demo.py [-h] {deploy,destroy,crawler}

Deploys S3 bucket, IAM role, creates Glue Crawler with test data Crawler joins json data with csv and stores in a parquet format in output prefix

positional arguments:
  {deploy,destroy,crawler}
                        deploy, creates componets named based on user supplied friendly name
                        destroy, removes existing componets based on user supplied friendly name
                        crawler, runs crawler on existing componets

options:
  -h, --help            show this help message and exit


## Testing
Data Generation Tests are unit tested and marked local_unit
Data Generation AWS Calls are mock tested and marked aws_moto
No AWS costs incur


pytest -m "not integ"
========================================================= test session starts =========================================================
platform linux -- Python 3.11.0, pytest-8.1.1, pluggy-1.4.0
rootdir: /home/ltm893/projects/aws-big-data
configfile: pyproject.toml
plugins: anyio-4.3.0
collected 7 items / 1 deselected / 6 selected

test/test_create_test_data.py ......                                                                                            [100%]

=================================================== 6 passed, 1 deselected in 1.54s ===================================================


---
Integegration tests creates cloud formation stack, glue componets and runs crawler. Then removes everythying will incur AWS costs


pytest -m "integ"
========================================================= test session starts =========================================================
platform linux -- Python 3.11.0, pytest-8.1.1, pluggy-1.4.0
rootdir: /home/ltm893/projects/aws-big-data
configfile: pyproject.toml
plugins: anyio-4.3.0
collected 7 items / 6 deselected / 1 selected

test/test_integ_scenario.py .                                                                                                   [100%]

============================================= 1 passed, 6 deselected in 332.95s (0:05:32) =============================================