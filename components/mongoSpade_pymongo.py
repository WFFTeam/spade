#mongoSpade_pymongo.py

import pymongo
import urllib
import re
import time
import os
import typing
import textwrap
import traceback
from urllib.parse import urlparse
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
from components.mongoSpade_stdout import *
from components.config_db import *

def mongodb_load_queries(load_num_queries):
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

    except Exception as err:
        print(red("ERROR --- Export of queries from DB interrupted"))
        print(yellow("Error code: ") + red(err))
        traceback.print_exception(type(err), err, err.__traceback__)
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
    except Exception as err:
        print(red("ERROR --- Export of fetched queries interrupted"))
        print(yellow("Error code: ") + red(err))
        traceback.print_exception(type(err), err, err.__traceback__)
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
    except Exception as err:
        print(red("ERROR --- Removal of finished queries failed"))
        print(yellow("Error code: ") + red(err))
        traceback.print_exception(type(err), err, err.__traceback__)
        print(" ")

def mongodb_google_results_import(fetched_query):
    collection_name = "google"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]

   #print(fetched_query) ###DEBUG

    try:
        google_results_collection = db_cm.insert_one(fetched_query)
        print(green(f'Results for {cyan(fetched_query["_id"])} ') + green(f'imported to collection {yellow(collection_name)}'))
        print(" ")

    except pymongo.errors.DuplicateKeyError as err:
        print(yellow(f'_id {red(fetched_query["_id"])}') + yellow(" already exists"))
        print(red('Skipping'))
       #traceback.print_exception(type(err), err, err.__traceback__)
        print(" ")
        return False

    except Exception as err:
        print(red(f'MongoDB import of URL list failed'))
        print(yellow("Error code: ") + red(err))
       #traceback.print_exception(type(err), err, err.__traceback__)
        print(" ")

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
    local_link_list = bs4_results_dict["LocalLinks"]
    ext_link_list = bs4_results_dict["ExtLinks"]

    if isinstance(local_link_list, list):
        local_link_counter = 0 + len(local_link_list)
    else:
        local_link_counter = 0

    if isinstance(ext_link_list, list):
        ext_link_counter = 0 + len(ext_link_list)
    else:
        ext_link_counter = 0

    _id = bs4_results_dict["_id"]
    try:
        bs4_results_collection = db_cm.insert_one(bs4_results_dict)
        print(green(f'Page title: {cyan(page_title[:123])} '))

        if email_count != 0:
            print(green(f'Found {cyan(email_count)} ') + green(f'e-mail addresses '))
        else:
            print(green(f'Found {red(email_count)} ') + green(f'e-mail addresses '))

        # if link_counter != 0:
        #     print(green(f'Found {cyan(link_counter)} ') + green('links '))
        # else:
        #     print(green(f'Found {red(link_counter)} ') + green('links '))

        if local_link_counter != 0:
            print(green(f'Found {cyan(local_link_counter)} ') + green('local links '))
        else:
            print(green(f'Found {red(local_link_counter)} ') + green('local links '))

        if ext_link_counter != 0:
            print(green(f'Found {cyan(ext_link_counter)} ') + green('external links '))
        else:
            print(green(f'Found {red(ext_link_counter)} ') + green('external links '))

        if db_cm.count_documents({ '_id': _id }, limit = 1) != 0:
            print(green(f'Successfully imported to collection {cyan(collection_name)} ') + green(f'with _id {cyan(_id)}'))
        else:
            print(red(f'Cant seem to find the imported document in the collection {cyan(collection_name)}' + red(f'with _id {cyan(_id)}')))

    except pymongo.errors.DuplicateKeyError as err:
        print(yellow(f'_id {red(_id)}') + yellow(" already exists"))
        print(red('Skipping'))
      # traceback.print_exception(type(err), err, err.__traceback__)
        print(" ")
        return False

    except Exception as e:
        print(red(f'MongoDB import of beautiful_soup_scrape failed'))
        print(yellow("Error details: ") + red(e))
        print(yellow("Error code: ") + red(e.code))
        traceback.print_exception(type(err), err, err.__traceback__)
        collection_name = "fails"
        db_cm = mng_db[collection_name]
        try:
            bs4_fails_collection = db_cm.insert_one(bs4_results_dict)
            print(yellow(f'Imported collection to {red(collection_name)} ') + yellow(f'with _id {red(_id)}'))
        except Exception as err:
            print(yellow(f'MongoDB secondary import to {red(collection_name)} ') + (yellow("failed")))
            pass

def mongodb_bs4_link_result_append(_id, bs_link_result_dict):
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    collection_name = "beautifulsoup"
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]

    link_id = bs_link_result_dict["link_id"]
    link_query = (link_id + ".link_id")
    
    json_time = json_timestamp()
    link_num = bs_link_result_dict["Num"]
    url_addr = bs_link_result_dict["URL"]
    title_text = bs_link_result_dict["Title"]
    page_title = re.sub(r'[\n\r\t]*', '', title_text)
    email_count = bs_link_result_dict["Mailnum"]
    found_mail = bs_link_result_dict["Email"]
    link_counter = bs_link_result_dict["Linknum"]
    link_list = bs_link_result_dict["Links"]
    link_list = list(set(link_list))
    link_host = bs_link_result_dict["Host"]

    try:
        
        bs4_link_append = db_cm.update_one(filter={'_id':_id}, update={'$addToSet':{ "processed": {'link_id': link_id, 'Timestamp': json_time, 'Num': link_num, 'URL': url_addr, 'Title': title_text, 'Mailnum': email_count, 'Email': found_mail, 'Linknum': link_counter, 'Links': link_list, 'Host': link_host}}}, upsert=True)
        print(green(f'      Link title: {cyan(page_title[:123])} '))
        if email_count != 0:
            print(green(f'      Found {cyan(str(email_count))} ') + green('e-mail addresses '))
        else:
            print(green(f'      Found {red(str(email_count))} ') + green('e-mail addresses '))
        if link_counter != 0:
            print(green(f'      Found {cyan(str(link_counter))} ') + green('links '))
        else:
            print(green(f'      Found {red(str(link_counter))} ') + green('links '))

        if db_cm.count_documents({"processed.link_id": link_id}, limit = 1) != 0:
            print(green("      Link ID ") + (cyan(link_id)) + (green(" appended to document ")) + (cyan(_id)))
        else:
            print(red(f'      Link ID: {link_id} in the collection {cyan(collection_name)}') + red(" not found"))
    except Exception as err:
        print(red(f'      Link ID {cyan(link_id)} {red("in document ")} {cyan(_id)} {red("failed")}'))
        print(yellow("      Error details: ") + red(err))
        traceback.print_exception(type(err), err, err.__traceback__)
        print(' ')
        pass

def mongodb_bs4_link_check(link_id):
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    collection_name = "beautifulsoup"
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    json_time = json_timestamp()
    link_query = (link_id + ".link_id")
    try:
        if db_cm.count_documents({ link_query: link_id }, limit = 1) != 0:
            return True
        else:
            return False
    except Exception as err:
        print(red(f'MongoDB query of link_id {cyan(link_id)} {red(" failed.")}'))
        print(yellow("Error details: ") + red(err))
        traceback.print_exception(type(err), err, err.__traceback__)
        pass

def mongodb_bs4_url_search(_id):
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    collection_name = "beautifulsoup"
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    json_time = json_timestamp()
    try:
        if db_cm.count_documents({ "_id": _id }, limit = 1) != 0:
            return True
        else:
            return False
    except Exception as err:
        print(red(f'MongoDB query of link_id {cyan(link_id)} {red(" failed.")}'))
        print(yellow("Error details: ") + red(err))
        traceback.print_exception(type(err), err, err.__traceback__)
        pass


### EXAMPLE: query_dict = {'Email':{"$not":{"$eq":'None'}}} ; collection = 'beautifulsoup' ; sort_list = "'Email', -1" ; display_dict = { 'Url': 1, 'Title': 1, 'Email': 1 }
###          show_results = mongodb_query_search( collection, query_dict, sort_list, display_dict )
def mongodb_query_search(collection, query_dict, sort_list, display_dict ):
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    collection_name = collection
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    json_time = json_timestamp()
    query_results_list = []
    query_results_instance = {}
    query_dict = dict(query_dict)

    try:
        if not sort_list and display_dict:
            query_results_pointer = db_cm.find(( query_dict ),( display_dict ))
        elif sort_list and not display_dict:
            query_results_pointer = db_cm.find(( query_dict )).sort( sort_list )
        elif not display_dict and not sort_list:
            query_results_pointer = db_cm.find(( query_dict ))
        else:
            query_results_pointer = db_cm.find(( query_dict ),( display_dict )).sort( sort_list )

        for item in query_results_pointer:
            try:
                query_results_instance = item
                query_results_instance = dict(query_results_instance)
                query_results_list.append(query_results_instance)

            except Exception as err:
                print(red(f'Function mongodb_query_search at {cyan(item)} {red(" failed.")}'))
                print(yellow("Error details: ") + red(err))
                traceback.print_exception(type(err), err, err.__traceback__)
                pass

    except Exception as err:
        print(red(f'MongoDB query of query {cyan(query_dict)} {red(" failed.")}'))
        print(yellow("Error details: ") + red(err))
        traceback.print_exception(type(err), err, err.__traceback__)
        pass

    return query_results_list


def mongodb_bs4_link_mail_append(_id, found_mail_list):
    _id_dict = {}
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    collection_name = "beautifulsoup"
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    found_mail_counter = 0 + len(found_mail_list)
    _id_dict = { '_id': _id }
    try:
        bs4_email_append = db_cm.update_one(filter=_id_dict, update={ '$set':{ 'Mailnum': found_mail_counter, 'Email': found_mail_list } })
        return True
    except Exception as err:
        print(red(f'MongoDB mail append of _id {cyan(_id)} {red(" failed.")}'))
        print(yellow("Error details: ") + red(err))
        traceback.print_exception(type(err), err, err.__traceback__)
        pass