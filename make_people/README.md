## Creates Random Test Data in S3 Bucket
File: people.json.gz
Format : [
  {
    "name": "Lktn Oycjtgukk", #  random strings
    "usage": "933", # random number
    "zip": "57379" # random US zip cocd 
  },
]

File: zip_group.csv.gz
Format: # random zip code, random number
60629, 3
79936, 8
11368, 10
90650, 1

# Usage
usage: create_objects.py [-h] --profile_name PROFILE_NAME --bucket_name BUCKET_NAME

options:
  -h, --help            show this help message and exit
  --profile_name PROFILE_NAME
                        AWS authenticated profile
  --bucket_name BUCKET_NAME
                        Unique bucket. Will remove and recreate bucket
(.venv) ltm893@River:make_people$

# Testing
Data Generation Tests are unit tested and marked local_unit
AWS Calls are mock tested and marked aws_moto

(.venv) ltm893@River:tests$ pytest 
================================================= test session starts ==================================================
platform linux -- Python 3.12.2, pytest-8.1.1, pluggy-1.4.0
rootdir: /home/ltm893/projects/aws-big-data/make_people/tests
configfile: pytest.ini
collected 6 items

test_create_objects.py ......                                                                                    [100%]

================================================== 6 passed in 1.58s ===================================================
(.venv) ltm893@River:tests$