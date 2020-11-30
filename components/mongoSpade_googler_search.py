#mongoSpade_googler_search.py

import urllib
import re
import unidecode
import time
from googlesearch import search
from urllib.error import HTTPError

from components.mongoSpade_stdout import *

def googler_search(googler_query, stop_after):
    i = 0
    progBarMult = i
    emptyBarMult = 70
    progSign = 1
    googler_search_result_list = []
    googler_query_sanitized = unidecode.unidecode(re.sub(r'\.+', "_", re.sub('[\W_]', '.', googler_query)))
    googler_query_short = ' '.join(googler_query_sanitized.split("_")[:12])

    try:
        for url in search(googler_query + ' -filetype:pdf ',   # The query you want to run
#                   tld = 'com',  # The top level domain
#                   lang = 'en',  # The language
#                   start = 0,    # First result to retrieve
                    stop = stop_after,    # Last result to retrieve
                    num = 10,     # Number of results per page
                    pause = 4.0,  # Lapse between HTTP requests
                    ):
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