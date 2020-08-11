#mongoSpade_query_google.py

import argparse
import urllib
import pymongo
import re
import json
import unidecode
import textwrap
import time
import os
import sys
import hashlib
import typing
from googlesearch import search
from urllib.error import HTTPError
from termcolor import colored
from datetime import datetime as dt
import warnings
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


# Nova funkcija za odbrojavanje posto stopwatch() baguje
def countdown(t):
    while t > 0:
        sys.stdout.write(red('\rWaiting for : {}s'.format(t)))
        t -= 1
        sys.stdout.flush()
        time.sleep(1)
    clear()

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
    collection_name = "queries"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    last_sequence = 0
    list_queries = []

    try:
        load_queries = db_cm.find().sort('qnum', 1).limit(int(load_num_queries))
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
    collection_name = "temp"
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
        print(" ")

def mongodb_query_delete(query_delete):
    collection_name = "queries"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    try:
        deleted_query = db_cm.delete_one(query_delete)
        deleted_query_num = str(query_delete).strip("{}").split(':')[1]
        print(green("Removed ") + cyan("query No." + deleted_query_num) + green(f' from collection {yellow(collection_name)}'))
    except Exception as error:
        print(red("ERROR --- Removal of finished queries failed"))
        print(yellow("Error code: ") + red(error))

def QueryProgress(currentLine, numOfLines, queryInput):
    print(" ")
    print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
    print(green(dt_print()))
    print(green(f"Fetching results for string: {googler_query}"))

def googler_search(googler_query, stop_after):
    i = 0
    progBarMult = i
    emptyBarMult = 70
    progSign = 1
    googler_search_result_list = []
    googler_query_sanitized = unidecode.unidecode(re.sub(r'\.+', "_", re.sub('[\W_]', '.', googler_query)))
    googler_query_short = ' '.join(googler_query_sanitized.split("_")[:12])

    try:
        if stop_after == 0:
            for url in search(googler_query + ' -filetype:pdf ',   # The query you want to run
    #                   tld = 'com',  # The top level domain
    #                   lang = 'en',  # The language
    #                   start = 0,    # First result to retrieve
    #                   stop = 10,    # Last result to retrieve
                        num = 10,     # Number of results per page
                        pause = 4.0,  # Lapse between HTTP requests
                        ):
    
    ############################################################################################ 
    # FIX DEMO MODE (stop_after)       
    #     else:
    #         for url in search(googler_query + ' -filetype:pdf ',   # The query you want to run
    #                     tld = 'com',  # The top level domain
    #                     lang = 'en',  # The language
    #                     start = 0,    # First result to retrieve
    #                     stop = stop_after,    # Last result to retrieve
    #                     num = 10,     # Number of results per page
    #                     pause = 4.0,  # Lapse between HTTP requests
    #                     ):
    ############################################################################################
                time.sleep(0.3)
                i += 1
                googler_search_result = [ i, url ]
                googler_search_result_list.append(googler_search_result)
                clear()
                print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))

                if len(googler_query_short) >= 122:
                    googler_query_short = ' '.join(googler_query_sanitized.split("_")[:12]) + "..."
                print(green("Searching Google and extracting results url for string ") + cyan(googler_query_short))
                print(green(f'No. {i} --- {yellow(googler_search_result[1])}'))
                if progBarMult == 100:
                    progSign = -1
                if progBarMult == 0:
                    progSign = 1
                progBarMult = progBarMult + 2 * progSign
                emptyBarMult = emptyBarMult - progSign
                emptyBar = " " * emptyBarMult
                print(' ')
                print(cyan(emptyBar + '<=' + "=" * progBarMult + '=>'))
                print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
            
            return googler_search_result_list
    except IndexError as e:
        print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
        print(red('Index error occured: ' + str(e.code)))
    except HTTPError as err:
        print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
        print(yellow("Error occured for string: ") + cyan(googler_query_short))
        print(red(err))
        if err.code == 429:
            print(red('Too many requests; temporarily blocked by Google'))
            print(yellow("Sleeping for 1200 secs"))
            countdown(1200)
            print(green("Retrying..."))
            googler_search(googler_query, stop_after)
    except Exception as error:
        print(red(f'ERROR --- Searching interrupted by exception'))
        print(yellow("Error code: ") + red(error))

def mongodb_google_results_import(fetched_query):
    collection_name = "google"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]

    try:
        google_results_collection = db_cm.insert_one(fetched_query)
        print(green(f'Results for {cyan(fetched_query["_id"])} ') + green(f'imported to collection {yellow(collection_name)}'))
        print(" ")

    except Exception as error:
        print(red(f'MongoDB import of URL list failed'))
        print(yellow("Error code: ") + red(error))

def mongodb_bs4_results_import(bs4_results_dict, error_flag):
    if error_flag is True:
        collection_name = "fails"
    elif error_flag == 'Skipped':
        collection_name = "skipped"
    else:
        collection_name = "beautifulsoup"
        
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    page_title = re.sub(r'[\n\r\t]*', '', bs4_results_dict["Title"])
    email_count = bs4_results_dict["Mailnum"]
    link_counter = bs4_results_dict["Linknum"]
    _id = bs4_results_dict["_id"]
    try:
        bs4_results_collection = db_cm.insert_one(bs4_results_dict)
        print(green(f'Page title: {cyan(page_title[:123])} '))

        if email_count != 0:
            print(green(f'Found {cyan(email_count)} ') + green(f'e-mail addresses '))
        else:
            print(green(f'Found {red(email_count)} ') + green(f'e-mail addresses '))
        if link_counter != 0:
            print(green(f'Found {cyan(link_counter)} ') + green('links '))
        else:
            print(green(f'Found {red(link_counter)} ') + green('links '))

        if db_cm.count_documents({ '_id': _id }, limit = 1) != 0:
            print(green(f'Successfully imported to collection {cyan(collection_name)} ') + green(f'with _id {cyan(_id)}'))
        else:
            print(red(f'Cant seem to find the imported document in the collection {cyan(collection_name)}' + red(f'with _id {cyan(_id)}')))

    except Exception as error:
        print(red(f'MongoDB import of beautiful_soup_scrape failed'))
        print(yellow("Error details: ") + red(error))
        collection_name = "fails"
        db_cm = mng_db[collection_name]
        try:
            bs4_fails_collection = db_cm.insert_one(bs4_results_dict)
            print(yellow(f'Imported collection to {red(collection_name)} ') + yellow(f'with _id {red(_id)}'))
        except Exception as error:
            print(yellow(f'MongoDB secondary import to {red(collection_name)} ') + (yellow("failed")))
            pass

#######################################################
###WIP Link scrape results import
# def mongodb_bs4_links_append(_id, bs_link_result_dict):
#     dbuser = urllib.parse.quote_plus(db_u)
#     dbpass = urllib.parse.quote_plus(db_p)
#   
#     mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
#     mng_db = mng_client[database_name]
#     db_cm = mng_db[collection_name]
#     page_title = re.sub(r'[\n\r\t]*', '', bs_link_result_dict["Title"])
#     email_count = bs_link_result_dict["Mailnum"]
#     link_counter = bs_link_result_dict["Linknum"]
#     link_id = bs_link_result_dict["link_id"]
#     try:
#         bs4_link_collection = db_cm.insert_one(_id, bs_link_result_dict)
#         print(green(f'Link title: {cyan(page_title[:123])} '))
#         if email_count != 0:
#             print(green(f'Found {cyan(email_count)} ') + green(f'e-mail addresses '))
#         else:
#             print(green(f'Found {red(email_count)} ') + green(f'e-mail addresses '))
#         if link_counter != 0:
#             print(green(f'Found {cyan(link_counter)} ') + green('links '))
#         else:
#             print(green(f'Found {red(link_counter)} ') + green('links '))
#         if db_cm.count_documents({ 'link_id': link_id,  }, limit = 1) != 0:
#             print(green(f'Successfully appended link scrape results to original document ') + green(f'with _id {cyan(_id)}'))
#         else:
#             print(red(f'Cant seem to find the imported document in the collection {cyan(collection_name)}' + red(f'with _id {cyan(_id)}')))
#######################################################

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--number", "-n", help="--number [number of queries to load from database]")
    parser.add_argument("--wait", "-w", help="--wait [time in seconds to pause between queries]")
    parser.add_argument("--stop", "-s", help="--stop [number of results after which to continue to next query]")
    args = parser.parse_args()
    collection_name = "queries" 
    if args.number:
        load_num_queries = args.number
    else:
        load_num_queries = 1
    if args.wait:
        wait_time = int(args.wait)
    else:
        wait_time = 5
    if args.stop:
        stop_after = int(args.stop)
    else:
        stop_after = 0
    print(yellow(dt_print()) + green("  ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
    print(green(f'Grabing queries from collection {yellow(collection_name)} ') + green(f'in database {yellow(database_name)} ') + green(f' on mongoDB host {yellow(dbhost)}'))
    print(green("Grabing ") + cyan(load_num_queries) + green(" queries from MongoDB"))
    queries = mongodb_query_search(load_num_queries)
    qnum = 0
    if not queries:
        print(yellow("No queries in specified collection"))
        print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))

    for query in queries:

        if query != "!!!ERROR!!!":
            _id = query["_id"]
            seq_num = query["qnum"]
            json_time = json_timestamp()
            query_string = query["Query"]
            
            googler_query = re.sub(r'[\n\r\t]*', '', query_string)
            googler_search_result = googler_search(googler_query, stop_after)

            fetched_query = {'_id':_id, 'qnum': seq_num, 'Timestamp': json_time, 'Query': query_string, 'UrlList': googler_search_result}
            if fetched_query["UrlList"] != 'null':
                query_delete = {'qnum': seq_num}
                mongodb_completed_query_copy(fetched_query)
                mongodb_query_delete(query_delete)
                mongodb_google_results_import(fetched_query)


                UrlList = googler_search_result
                crawled_url_list = []
                skipped_url_list = []
                successful_crawl_count = url_count = bs_error_count = mail_count = skipped_url_count = 0
                try:

                    for i in UrlList:
                        url = i[1]
                        url_count += 1
                        _id = hashlib.md5(url.encode('utf-8')).hexdigest()
                        json_time = json_timestamp()
                        bs_result = beautifulsoup_scrape(url)

                        if bs_result[2] == "!!!SKIPPED!!!":
                            error_flag = 'Skipped'
                            title_text = "Skipped"
                            found_mail = "Skipped"
                            link_list = "Skipped"
                            urlparse_host = bs_result[3]
                            email_count = 0
                            link_counter = 0
                            skipped_url_count += 1
                            url_addr = url
                            skipped_url_list.append(url_addr)
                            bs4_results = ([skipped_url_count, json_time, url_addr, title_text, found_mail, link_list])
                            print(yellow(dt_print()) + yellow("  ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
                            print(yellow(f'Scrape of {yellow(url_addr[:132])} ') + (red("skipped")))
                            print(red("Host found in config_skips file"))
                            print(yellow("Host: ") + red(urlparse_host))
                            bs4_results_dict = ({'_id':_id, 'Timestamp': json_time, 'Num': skipped_url_count, 'URL': url_addr, 'Title': title_text, 'Mailnum': email_count, 'Email': found_mail, 'Linknum': link_counter, 'Links': link_list})
                            mongodb_bs4_results_import(bs4_results_dict, error_flag)
                            print(' ')

                        elif bs_result[2] != "!!!ERROR!!!":
                            error_flag = False
                            successful_crawl_count += 1
                            title_text = bs_result[0]
                            found_mail = bs_result[1]
                            link_list = bs_result[2]
                            urlparse_host = bs_result[3]
                            email_count = 0 + len(found_mail)
                            if email_count == 0:
                                found_mail = 'None'
                            link_counter = 0 + len(link_list)
                            if link_counter == 0:
                                link_list = 'None'
                            url_addr = url
                            crawled_url_list.append(url_addr)
                            bs4_results = ([successful_crawl_count, json_time, url_addr, title_text, found_mail, link_list])
                            bs4_results_dict = ({'_id':_id, 'Timestamp': json_time, 'Num': successful_crawl_count, 'URL': url_addr, 'Title': title_text, 'Mailnum': email_count, 'Email': found_mail, 'Linknum': link_counter, 'Links': link_list})
                            
                            print(yellow(dt_print()) + green("  ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
                            print(green(f'Crawling URL {yellow(url_addr[:132])} '))
                            mongodb_bs4_results_import(bs4_results_dict, error_flag)
                            print(' ')

                            #######################################################
                            ### FOUND LINK CRAWL --- WIP
                            # link_num = 0
                            # if isinstance(link_list, list):
                            #     for url in link_list:
                            #         link_url = url
                            #         link_id = hashlib.md5(link_url.encode('utf-8')).hexdigest()
                            #         json_time = json_timestamp()
                            #         bs_link_result = beautifulsoup_scrape(link_url)
                            #         link_num += 1
                            #         if bs_link_result[2] != "!!!ERROR!!!":
                            #             title_text = bs_link_result[0]
                            #             found_mail = bs_link_result[1]
                            #             link_list = bs_link_result[2]
                            #             urlparse_host = bs_link_result[3]
                            #             email_count = 0 + len(found_mail)
                            #             if email_count == 0:
                            #                 found_mail = 'None'
                            #             link_counter = 0 + len(link_list)
                            #             if link_counter == 0:
                            #                 link_list = 'None'
                            #             url_addr = link_url
                            #             crawled_link_list.append(url_addr)
                            #             bs_link_result_dict = ({'link_id': link_id, 'Timestamp': json_time, 'Num': link_num, 'URL': url_addr, 'Title': title_text, 'Mailnum': email_count, 'Email': found_mail, 'Linknum': link_counter, 'Links': link_list})
                            #             print(yellow(dt_print()) + green("  ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
                            #             print(green(f'Crawling link {yellow(url_addr[:132])} '))
                            #             mongodb_bs4_links_append(_id, bs_link_result_dict)
                            #             print(' ')
                            #
                            ######################################################
                        else:
                            error_flag = True
                            url_addr = url
                            error = bs_result[1]
                            bs_error_count += 1
                            url_addr = url
                            print(yellow(dt_print()) + red("  ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
                            print(yellow(f'Scrape of {red(url_addr[:132])} ') + (yellow("failed")))
                            print(yellow("Error: beautifulsoup_scrape function encountered an error."))
                            print(yellow("Error details: ") + red(error))
                            bs4_results_dict = ({'_id':_id, 'Timestamp': json_time, 'Num': bs_error_count, 'URL': url_addr, 'Title': "bs4_Error", 'Mailnum': 0, 'Email': 'None', 'Linknum': 0, 'Links': 'None', 'ErrorInfo': error})
                            mongodb_bs4_results_import(bs4_results_dict, error_flag) ### FAILED URL CRAWL TRACKING
                            print(' ')

                        time.sleep(1)
                except Exception as error:
                    print(red(f'Error occured during iteration throught the list of URLS'))
                    print(yellow("Error details: ") + red(error))
                    collection_name = "fails"
                   ######################################################
                   # EXCEPTION NOT WORKING CORRECTLY
                   #db_cm = mng_db[collection_name]
                   #try:
                   #    bs4_fails_collection = db_cm.insert_one(bs4_results_dict)
                   #    print(yellow(f'Imported collection to {red(collection_name)} ') + yellow(f'with _id {red(_id)}'))
                   #except Exception as error:
                   #    print(yellow(f'MongoDB secondary import to {red(collection_name)} ') + (yellow("failed")))
                   #pass
                   ######################################################
        else:
            print(yellow(dt_print()) + red("  ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
            print(red("ERROR --- Query result atypical."))
            print(yellow("Error code: ") + red(query))

        total_urls = skipped_url_count + successful_crawl_count + bs_error_count
        print(cyan(dt_print()) + yellow("  ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
        print(cyan("From " + total_urls + " URLs, ") + (red(bs_error_count)) + red(" failed, ") + yellow(skipped_url_count) + yellow(" skipped and ") + green(successful_crawl_count) + green(" successfully crawled."))

        countdown(wait_time)


if __name__ == "__main__":
    main()