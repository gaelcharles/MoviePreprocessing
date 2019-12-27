import requests
import sys
import os
import pandas as pd
from datetime import datetime

## DATA PREPROCESSING FUNCTIONS
# Some basic data preprocessing. We can imagine any other preprocessing here

def average_rating(rating_array):
    value, values = None, None
    average = 0
    for rate in rating_array:
        rate = rate['Value']
        
        if '/' in rate:
            values = rate.split('/')
            value = 100*float(values[0]) / float(values[1])
        elif '%' in rate:
            value = float(rate.replace('%',''))
        else:
            assert False
        
        average += value
    return average / len(rating_array)

def runtime_min(runtime):
    return int(runtime.split(' ')[0])

def main_genre(genres):
    return genres.split(',')[0]

def release_date(release):
    return datetime.strptime(release, '%d %b %Y').date()

## MOVIE SEARCH
# We take the first result (most permanent according to IMDB) only.
# For example, if we search for 'revenge', most permanent result is 'Star Wars: Episode III - Revenge of the Sith'.
# If no result is found, nothing is added to the CSV.

API_KEY = '68d8e88'

search = requests.get("http://www.omdbapi.com",
                      params= {"apikey": API_KEY,
                               "s": sys.argv[1]})

# If we have at least one result
if search.json()['Response'] == 'True':
    
    ## RETRIEVE FIRST RESULT
    
    imdbID = search.json()['Search'][0]['imdbID']
    response_json = requests.get("http://www.omdbapi.com",
                                params= {"apikey": API_KEY,
                                         "i": imdbID}).json()

    ## APPEND TO DATAFRAME
    
    movies_df = pd.read_csv('movies.csv') if os.path.exists('movies.csv') else pd.DataFrame()
    movies_df = movies_df.append(pd.DataFrame({
        'title':          [response_json['Title']], 
        'release_date':   [release_date(response_json['Released'])], 
        'runtime_min':    [runtime_min(response_json['Runtime'])], 
        'main_genre':     [main_genre(response_json['Genre'])], 
        'average_rating': [average_rating(response_json['Ratings'])], 
        'plot':           [response_json['Plot']]
    }))
    
    ## EXPORT TO CSV

    movies_df.to_csv('movies.csv', index=False)
    
    ## PUT IN HDFS
    
    try:
        os.system("hadoop fs -put -f ~/hadoop-project/movies.csv hadoop-project/movies.csv")
    except Exception as e:
        print(e)
