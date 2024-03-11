import argparse

from pyspark.sql import SparkSession

import json
import random
import string
from uszipcode import SearchEngine

lower_list = list(string.ascii_lowercase)
upper_list = list(string.ascii_uppercase)


zip_search = SearchEngine(simple_zipcode=False)
all_zip_codes = zip_search.by_pattern('', returns=200000)


def make_random_string(max_length):
    if max_length < 3 :
        return("needs to be larger than 2")
    r_string = random.choice(upper_list)
    rr = random.randrange(3,max_length)
    #print(rr)
    for l in random.choices(lower_list,k=rr):
        r_string = r_string + l
    return r_string

def make_random_number_string(max_length):
    if max_length < 2 :
        return("needs to be larger than 1")
    first_digit_choices = [ str(i) for i in range(1,10) ]
    other_digit_choices = [ str(i) for i in range(10) ]
    rr = random.randrange(3,max_length)
    r_string = random.choice(first_digit_choices)
    for l in random.choices(other_digit_choices,k=rr -1):
        r_string = r_string + l
    return r_string

def get_random_zip():
    return random.choice(all_zip_codes)

    
def make_person():
    person = {}
    person['name'] = make_random_string(6) + ' ' + make_random_string(12)
    person['usage'] =  make_random_number_string(6)
    person['zip']  = get_random_zip()
    return person

def make_zip_rating():
    zip_rating = [ [z, random.randrange(1,11)] for z in all_zip_codes ]



def make_k_people(output_uri):
        with SparkSession.builder.appName("Make 1k People").getOrCreate() as spark:
            one_k_people = [make_person() for i in range(1,1000) ]
            # one_k_people_json = json.dumps(one_k_people)
            df = spark.createDataFrame(one_k_people)
            df.write.json(output_uri)


#with open("sample.json", "w") as outfile:
#    outfile.write(one_k_people_json)


''' 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    #parser.add_argument(
    #    '--data_source', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    make_k_people(args.output_uri)

'''