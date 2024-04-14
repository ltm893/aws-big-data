
from uszipcode import SearchEngine

zip_search = SearchEngine()
all_zips = [obj.zipcode  for obj in  zip_search.by_population(lower=50, upper=120000,returns=100000) ]
  

