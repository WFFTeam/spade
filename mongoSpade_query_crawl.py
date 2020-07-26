#mongoSpade_query_crawl.py

import argparse
import urllib
import pymongo
import re
import json
import unidecode
import textwrap
import time
import os
from googlesearch import search
from urllib.error import HTTPError
from termcolor import colored
from datetime import datetime as dt
import warnings # pymongo warning ingore
warnings.filterwarnings("ignore", category=DeprecationWarning)

from components import *
from components.config_db import *

# TERMCOLOR FUNCTIONS
def yellow(text):
    return colored(text, 'yellow', attrs=['bold'])
def green(text):
    return colored(text, 'green', attrs=['bold'])
def red(text):
    return colored(text, 'red', attrs=['bold'])
def cyan(text):
    return colored(text, 'cyan', attrs=['bold'])

# SCREEN CLEAN FUNCTION
def clear():
    os.system( 'clear')

# CURRENT DATE&TIME FUNCTION
def dt_print():
    now = dt.now()
    dt_string = now.strftime("%H:%M:%S %d/%m/%Y")
    return dt_string

def stopwatch(sec):
    while sec:
        minn, secc = divmod(sec, 60)
        timeformat = '{:02d}:{:02d}'.format(minn, secc)
        print(timeformat, end='')
        time.sleep(1)
        sec -= 1
    print("Continuing!")

def json_timestamp():
    now = dt.now()
    json_timestamp = now.isoformat()
    return json_timestamp

def mongodb_query_search(load_num_queries):
    collection_name = "QUERIES"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    last_sequence = 0
    list_queries = []

    try:
        load_queries = db_cm.find().sort('SequenceNum', 1).limit(int(load_num_queries))
        for item in load_queries:
            last_sequence = item
            list_queries.append(last_sequence)
        return list_queries

    except Exception as error:
        print(red("ERROR --- Export of queries from DB interrupted"))
        print(yellow("Error code: ") + red(error))
        query_search_error = "!!!ERROR!!!"
        return query_search_error

def mongodb_completed_query_copy(fetched_query):
    collection_name = "TEMP"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    try:
        backup_query = db_cm.insert_one(fetched_query)
        print(green("Copied ") + cyan(fetched_query["_id"]) + green(" query to backup collection ") + cyan(collection_name))
    except Exception as error:
        print(red("ERROR --- Export of fetched queries interrupted"))
        print(yellow("Error code: ") + red(error))

def mongodb_query_delete(query_delete):
    collection_name = "QUERIES"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    try:
        deleted_query = db_cm.delete_one(query_delete)
        print(green("Deleted ") + cyan(query_delete) + green(f' query from collection {yellow(collection_name)}'))
        print(" ")
    except Exception as error:
        print(red(f'ERROR IN DB QUERY REMOVAL'))
        print(red(error))
        print(" ")



def QueryProgress(currentLine, numOfLines, queryInput):
    print(" ")
    print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
    print(green(dt_print()))
    print(green(f"Fetching results for string: {googler_query}"))

def googler_search(googler_query):
    i = 0
    progBarMult = i
    emptyBarMult = 70
    progSign = 1
    googler_search_result_list = []
    try:
        for url in search(googler_query,   # The query you want to run
#                   tld = 'com',  # The top level domain
#                   lang = 'en',  # The language
#                   start = 0,    # First result to retrieve
#                   stop = 10,    # Last result to retrieve
                    num = 10,     # Number of results per page
                    pause = 4.0,  # Lapse between HTTP requests
                    ):


            time.sleep(0.2)
            i += 1
            googler_search_result = ([i, url])
            googler_search_result_list.append(googler_search_result)
            clear()
            print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))

            googler_query_sanitized = unidecode.unidecode(re.sub(r'\.+', "_", re.sub('[\W_]', '.', googler_query)))
            googler_query_short = '_'.join(googler_query_sanitized.split("_")[:12]) + "..."
            print(green("Searching Google and extracting results url for string ") + cyan(googler_query_short))
            print(green(f'No. {i} --- {yellow(googler_search_result[1])}'))
            if progBarMult == 100:
                progSign = -1
            if progBarMult == 0:
                progSign = 1
            progBarMult = progBarMult + 2 * progSign
            emptyBarMult = emptyBarMult - progSign
            emptyBar = " " * emptyBarMult
            print(" ")
            print(cyan(emptyBar + '<=' + "=" * progBarMult + '=>'))
            print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
          # print(yellow("Current list of url:") + cyan(googler_search_result))
          # print(yellow("Appended list of url:") + cyan(googler_search_result_list))
        
        return googler_search_result_list
    except IndexError as e:
        print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
        print(red('Index error occured: ' + str(e.code)))
    except HTTPError as err:
        print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
        print(red(err))
        if err.code == 429:
            print(red('Too many requests; temporarily blocked by Google'))
            print(yellow("Sleeping for 600 secs"))
            stopwatch(600)
    except Exception as error:
        print(red(f'ERROR --- Searching interrupted by exception'))
        print(yellow("Error code: ") + red(error))

def mongodb_google_results_import(fetched_query):
    collection_name = "GOOGLE"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]

    try:
        google_results_collection = db_cm.insert_one(fetched_query)
        print(green(f'Results for {cyan(fetched_query["_id"])} imported to collection {yellow(collection_name)}'))

    except Exception as error:
        print(red(f'ERROR --- Import of queries into DB interrupted by error'))
        print(yellow("Error code: ") + red(error))

def mongodb_bs4_results_import(bs4_results):
    collection_name = "BEAUTIFULSOUP"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]

    try:
        bs4_results_collection = db_cm.insert_one(bs4_results_dict)
        print(green(f'Results for {cyan(bs4_results["Title"])} imported to collection {yellow(collection_name)}'))

    except Exception as error:
        print(red(f'ERROR --- Import of bs4 output into DB interrupted by error'))
        print(yellow("Error code: ") + red(error))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--number", "-n", help="--number [number of queries to load from database]")
    args = parser.parse_args()
    collection_name = "QUERIES" 
    if args.number:
        load_num_queries = args.number
    else:
        load_num_queries = 1
    print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
    print(green(f'Grabing queries from collection {yellow(collection_name)} in database {yellow(database_name)} from mongoDB host {yellow(dbhost)}'))
    print(green("Grabing ") + cyan(load_num_queries) + green(" queries from MongoDB"))
    queries = mongodb_query_search(load_num_queries)
    SequenceNum = 0

    for query in queries:
        googler_search_result_list = []
        if query != "!!!ERROR!!!":
            _id = query["_id"]
            seq_num = query["SequenceNum"]
            json_time = query["Timestamp"]
            query_string = query["Query"]
            
            googler_query = re.sub(r'[\n\r\t]*', '', query_string)
            googler_search_result = googler_search(googler_query)
            googler_search_result_list.append(googler_search_result)

            fetched_query = {'_id':_id, 'SequenceNum': seq_num, 'Timestamp': json_time, 'Query': query_string, 'UrlList': googler_search_result_list }
            if fetched_query["UrlList"] != 'null':
                query_delete = {'SequenceNum': seq_num}
                mongodb_completed_query_copy(fetched_query)
                mongodb_query_delete(query_delete)
                mongodb_google_results_import(fetched_query)
                UrlList = googler_search_result_list
                for url in UrlList:
                    bs_result = beautifulsoup_scrape(url)
                    if bs_result[2] != "!!!ERROR!!!":
                        title_text = bs_result[0]
                        found_mail = bs_result[1]
                        link_list = bs_result[2]
                        url_addr = url
                        bs4_results = ([title_text, found_mail, link_list])
                        bs4_results_dict = ({'Title': title_text, 'Email': found_mail, 'Links': link_list})
                        bs4_results_list.append(bs4_results)
                        mongodb_bs4_results_import(bs4_results_dict)


        else:
            print(red("ERROR --- Query result atypical."))
            print(yellow("Error code: ") + red(query))
            pass
    time.sleep(4)


if __name__ == "__main__":
    main()