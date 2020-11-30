#mongoSpade_query_import.py

import argparse
import urllib
import pymongo
import re
import json
import unidecode
from termcolor import colored
from datetime import datetime as dt
import warnings # pymongo warning ingore
warnings.filterwarnings("ignore", category=DeprecationWarning)

from components.mongoSpade_stdout import *
from components.config_db import *
               
def mongodb_query_input(query_dict):
    collection_name = "queries"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]

    try:
        queries_db = db_cm.insert_one(query_dict)
        print(green(f'String {cyan(query_dict["Query"])} imported as string number {yellow(query_dict["qnum"])}'))

    except Exception as error:
        print(red(f'ERROR --- Import of query into database failed'))
        print(red("Error code: ") + yellow(error))
        print(' ')
        return "!!!ERROR!!!"

def mongodb_query_search():
    collection_name = "queries"
    dbuser = urllib.parse.quote_plus(db_u)
    dbpass = urllib.parse.quote_plus(db_p)
    
    mng_client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/%s' % (dbuser, dbpass, dbhost, dbport, user_db))
    mng_db = mng_client[database_name]
    db_cm = mng_db[collection_name]
    last_sequence = 0

    try:
        list_sequence = db_cm.find().sort('qnum', -1).limit(1)
        for item in list_sequence:
            last_sequence = (str(item).split(",")[1].split(":")[1].lstrip())
        return last_sequence

    except Exception as error:
        print(red(f'ERROR --- Sequence number enumeration of the last query failed'))
        print(yellow("Error code: ") + red(error))
        return 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", "-l", help="--list [path to list file to read search queries from, one query per line]")
    parser.add_argument("--query", "-q", help="--query [search string]")
    parser.add_argument("--show", "-s", help="--show [search string]")
    args = parser.parse_args()
    if args.list or args.query:
        print(green("Importing query(es) to MongoDB"))
        mongoDBstring = "mongodb://" + dbhost.replace("'", "") + ":" + str(dbport)
        last_num = 0
        last_num = int(mongodb_query_search())
        no_of_successful_import = 0
        print(cyan(last_num) + green(" queries pending for crawling"))
        print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
        if args.list:
            query_file_path = args.list
            file_lines = sum(1 for line in open(query_file_path, 'r'))
            with open(query_file_path, 'r') as query_file:
                queries = []
                error_count = 0
                if last_num != 0:
                    seq_num = 0 + last_num
                else:
                    seq_num = 0
                print(green("Following queries added to DB:"))
                for line in query_file:
                    query = (re.sub(r'[\n\r\t]*', '', line))
                    queries.append(query)
                for query in queries:
                    seq_num += 1
                    sanitized_query = unidecode.unidecode(re.sub(r'\.+', "_", re.sub('[\W_]', '.', query)))
                    _id = '_'.join(sanitized_query.split("_")[:12])
                    json_time = json_timestamp()
                    query_dict = {'_id': _id, 'qnum': seq_num, 'Timestamp': json_time, 'Query': query }
                    query_input = mongodb_query_input(query_dict)
                    if query_input == "!!!ERROR!!!":
                        seq_num -= 1
                        error_count += 1
                    else:
                        no_of_successful_import += 1

                collection_name = "queries"
                print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
                print(green("DB host: ") + yellow(dbhost))
                print(green("Database name: ") + yellow(database_name))
                print(green("Collection name: ") + yellow(collection_name))
                print(yellow(no_of_successful_import) + green(" queries added to MongoDB pending list."))
                if error_count != 0:
                    print(red(error_count) + red(" errors encountered during the import."))
                else:
                    print(green("No errors occured during the import of queries"))
        elif args.query:
            print(green("Following queries added to DB:"))
            if last_num != 0:
                seq_num = 0 + last_num
            else:
                seq_num = 0
                
            line = args.query
            query = (re.sub(r'[\n\r\t]*', '', line))
            sanitized_query = unidecode.unidecode(re.sub(r'\.+', "_", re.sub('[\W_]', '.', query)))
            _id = '_'.join(sanitized_query.split("_")[:12])
            seq_num = last_num +1
            json_time = json_timestamp()
            query_dict = {'_id': _id, 'qnum': seq_num, 'Timestamp': json_time, 'Query': query }
            query_input = mongodb_query_input(query_dict)
            if query_input == "!!!ERROR!!!":
                seq_num -= 1
            else:
                no_of_successful_import += 1
    elif args.show:
        print(green("Searching in MongoDB"))
        print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
        print(green("String: ") + yellow(args.show))

if __name__ == "__main__":
    main()