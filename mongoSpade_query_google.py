#!/usr/bin/env python

#mongoSpade_query_google.py

import argparse
import re
import time
import hashlib
import typing
import textwrap
import traceback
import signal
import sys
from components import *

### TODO: Keyboard interrupt handler --------------------------------------------------------------------
def signal_handler(signal, frame):
  # your code here
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
### TODO: Keyboard interrupt handler --------------------------------------------------------------------

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
    print(yellow(dt_print()) + yellow("  ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
    print(green(f'Grabing queries from collection {yellow(collection_name)} ') + green(f'in database {yellow(database_name)} ') + green(f' on mongoDB host {yellow(dbhost)}'))
    print(green("Grabing ") + cyan(load_num_queries) + green(" queries from MongoDB"))
    queries = mongodb_load_queries(load_num_queries)
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
                mongodb_google_results_import(fetched_query)


                UrlList = googler_search_result
                num_url = 0 + len(UrlList)

                crawled_url_list = []
                skipped_url_list = []
                successful_crawl_count = url_count = bs_error_count = mail_count = skipped_url_count = 0
                try:
                    for i in UrlList:
                        url = i[1]
                        url_count += 1
                        _id = hashlib.md5(url.encode('utf-8')).hexdigest()
                        json_time = json_timestamp()
                        url_already_crawled = mongodb_bs4_url_search(_id)
                        if url_already_crawled is True:
                            print(yellow(dt_print()) + green("  ||  URL ") + red(str(url_count) + " of " + str(num_url)) + yellow(" ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
                            print(yellow("Url: ") + red(url))
                            print(yellow("Url already crawled and in database, skipping"))
                            skipped_url_count += 1
                            print(" ")
                            continue

                        else:
                            found_mail_list = []
                            bs_result = beautifulsoup_scrape(url)
                            if bs_result[2] == "!!!SKIPPED!!!":
                                error_flag = 'Skipped'
                                title_text = "Skipped"
                                found_mail = "Skipped"
                                link_list = "Skipped"
                                local_link_list = "Skipped"
                                ext_link_list = "Skipped"
                                urlparse_host = bs_result[3]
                                url_domain = urlparse_host
                                email_count = 0
                                link_counter = 0
                                skipped_url_count += 1
                                url_addr = url
                                skipped_url_list.append(url_addr)
                                bs4_results = ([skipped_url_count, json_time, url_addr, title_text, found_mail, link_list])
                                print(yellow(dt_print()) + yellow("  ||  URL ") + green(str(url_count) + " of " + str(num_url)) + yellow(" ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
                                print(yellow("Skipping host: ") + red(urlparse_host))
                                bs4_results_dict = ({'_id':_id, 'Timestamp': json_time, 'Num': skipped_url_count, 'URL': url_addr, 'Title': title_text, 'Mailnum': email_count, 'Email': found_mail, 'Linknum': link_counter, 'Links': link_list, 'LocalLinks': local_link_list, 'ExtLinks': ext_link_list})
                                mongodb_bs4_results_import(bs4_results_dict, error_flag)
                                print(' ')

                            elif bs_result[2] != "!!!ERROR!!!":
                                error_flag = False
                                successful_crawl_count += 1
                                title_text = bs_result[0]
                                found_mail = bs_result[1]
                                link_list = bs_result[2]
                                local_link_list = []
                                ext_link_list = []
                                urlparse_host = bs_result[3]
                                url_domain = urlparse_host
                                if isinstance(link_list, list): ## TEST ## local_link_list, ext_link_list
                                    for url in link_list:   ## TEST
                                        link_url_check = url ## TEST
                                        if url_domain in link_url_check:    ## TEST
                                            local_link_list.append(link_url_check) ###Check if local link
                                        else:
                                            ext_link_list.append(link_url_check)
                                if isinstance(found_mail, list):
                                    email_count = 0 + len(found_mail)
                                else:
                                    email_count = 0
                                if email_count == 0:
                                    found_mail = 'None'
                                local_link_counter = 0 + len(local_link_list)
                                if local_link_counter == 0:
                                    local_link_list = 'None'
                                ext_link_counter = 0 + len(ext_link_list)
                                if ext_link_counter == 0:
                                    ext_link_list = 'None'
                                link_counter = 0 + len(link_list)
                                if link_counter == 0:
                                    link_list = 'None'
                                url_addr = url
                                crawled_url_list.append(url_addr)
                                if found_mail != 'None':
                                    found_mail_list.extend(found_mail)

                                bs4_results = ([successful_crawl_count, json_time, url_addr, title_text, found_mail, local_link_list, ext_link_list])
                                bs4_results_dict = ({'_id':_id, 'Timestamp': json_time, 'Num': successful_crawl_count, 'URL': url_addr, 'Title': title_text, 'Mailnum': email_count, 'Email': found_mail, 'Linknum': link_counter, 'LocalLinks': local_link_list, 'ExtLinks': ext_link_list})
                                
                                print(yellow(dt_print()) + green("  ||  URL ") + green(str(url_count) + " of " + str(num_url)) + yellow(" ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
                                print(green(f'Crawling URL {yellow(url_addr[:132])} '))
                                mongodb_bs4_results_import(bs4_results_dict, error_flag)
                                print(' ')

                                link_num = link_found_mail_counter = 0
                                crawled_link_list = []
                                link_found_mail_list = []
                                if isinstance(local_link_list, list):
                                    for url in local_link_list:
                                        try:
                                            link_email_count = 0
                                            link_url = url
                                            link_id = hashlib.md5(link_url.encode('utf-8')).hexdigest()
                                            json_time = json_timestamp()
                                            link_already_crawled = mongodb_bs4_link_check(link_id)
                                            link_num += 1
                                            skipped_link_count = 0
                                            if link_already_crawled is not True:
                                                bs_link_result = beautifulsoup_scrape(link_url)
                                                time.sleep(0.3)
                                                if bs_link_result[2] == "!!!SKIPPED!!!":
                                                    error_flag = 'Skipped'
                                                    link_title_text = "Skipped"
                                                    link_found_mail = "Skipped"
                                                    link_link_list = "Skipped"
                                                    link_urlparse_host = bs_result[3]
                                                    link_email_count = 0
                                                    link_link_counter = 0
                                                    skipped_link_count += 1
                                                    link_url_addr = link_url

                                                    print(green(f'      Crawling link {yellow(str(link_num))}') + green(f' of {yellow(str(local_link_counter))}'))
                                                    print(green(f'      Link URL: {yellow(link_url_addr[:132])}'))
                                                    print("      " + cyan(link_id) + yellow(' link skipped'))
                                                    print(' ')
                                                elif bs_link_result[2] == "!!!ERROR!!!":
                                                    error_flag = True
                                                    link_url_addr = link_url
                                                    error = bs_link_result[1]
                                                    print(green(f'      Crawling link {yellow(link_num)}') + green(f' of {yellow(str(local_link_counter))}'))
                                                    print(green(f'      Link URL: {yellow(link_url_addr[:132])}'))
                                                    print("      " + red(f'Link {link_id} scrape failed'))
                                                    print(yellow("      Error details: ") + red(error))
                                                    print(' ')

                                                else:
                                                    link_title_text = bs_link_result[0]
                                                    link_found_mail = bs_link_result[1]
                                                    link_link_list = bs_link_result[2]
                                                    link_urlparse_host = bs_link_result[3]
                                                    link_email_count = 0 + len(link_found_mail)
                                                    if link_email_count == 0:
                                                        link_found_mail = 'None'
                                                    else:
                                                        link_found_mail_list.extend(link_found_mail)
                                                        link_found_mail_counter = link_found_mail_counter + link_email_count
                                                    link_link_counter = 0 + len(link_link_list)
                                                    if link_link_counter == 0:
                                                        link_link_list = 'None'
                                                    link_url_addr = link_url
                                                    crawled_link_list.append(link_url_addr)
                                                    bs_link_result_dict = ({'link_id': link_id, 'Timestamp': json_time, 'Num': link_num, 'URL': link_url_addr, 'Title': link_title_text, 'Mailnum': link_email_count, 'Email': link_found_mail, 'Linknum': link_link_counter, 'Links': link_link_list, 'Host': link_urlparse_host})
                                                    print(green(f'      Crawling link {yellow(str(link_num))}') + green(f' of {yellow(str(local_link_counter))}'))
                                                    print(green(f'      Link URL: {yellow(link_url_addr[:132])}'))
                                                    mongodb_bs4_link_result_append(_id, bs_link_result_dict)
                                                    print(' ')
                                                
                                            elif link_already_crawled is True:
                                                link_url_addr = link_url
                                                print(green(f'      Crawling link {yellow(str(link_num))}') + green(f' of {yellow(str(local_link_counter))}'))
                                                print(green(f'      Link URL: {yellow(link_url_addr[:132])}'))
                                                print("      " + cyan(link_id) + red(' link ID already crawled'))
                                                print(' ')

                                            else:
                                                link_url_addr = link_url
                                                print(green(f'      Crawling link {yellow(str(link_num))}') + green(f' of {yellow(str(local_link_counter))}'))
                                                print(green(f'      Link URL: {yellow(link_url_addr[:132])}'))
                                                print("      " + cyan(link_id) + red(' link_already_crawled error'))
                                                print("      " + red("Variable link_already_crawled value is: ") + red(str(link_already_crawled)))
                                                print(' ')

                                        except Exception as err:
                                            print(yellow(dt_print()) + red("  ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
                                            print(yellow(f'Scrape of link {red(link_url[:132])} ') + (yellow("failed")))
                                            print(red(f'Error: {err}'))
                                            traceback.print_exception(type(err), err, err.__traceback__)
                                            print(yellow(f'On to the next one'))
                                            pass

                                    if link_found_mail_list == "[[]]":
                                        link_found_mail_list = ''
                                    if link_found_mail_counter == 0:
                                        link_found_mail_list = 'None'
                                    else:
                                        link_found_mail_list = list(set(link_found_mail_list))
                                        found_mail_list.extend(link_found_mail_list)
                                        found_mail_list = list(set(found_mail_list))
                                        link_found_mail_counter = 0 + len(link_found_mail_list)
                                        total_found_mail_counter = 0 + len(found_mail_list)
                                        mongodb_bs4_link_mail_append_status = mongodb_bs4_link_mail_append(_id, found_mail_list)
                                        if mongodb_bs4_link_mail_append_status is True:
                                            print(green(f'      Appended {yellow(str(link_found_mail_counter))}') + green(f' mail addresses found in local links to {yellow(str(_id))}'))
                                            print('')
                                        else:
                                            print(red(f'      Error during link mail uppend to ') + yellow(str(_id)))

                            else:
                                error_flag = True
                                url_addr = url
                                error = bs_result[1]
                                bs_error_count += 1
                                print(red(dt_print()) + red("  ||  URL ") + yellow(str(url_count) + " of " + str(num_url)) + red(" ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
                                print(yellow(f'Scrape of {red(url_addr[:132])} ') + (yellow("failed")))
                                print(yellow("Error: beautifulsoup_scrape function encountered an error."))
                                print(yellow("Error details: ") + red(error))
                                bs4_results_dict = ({'_id':_id, 'Timestamp': json_time, 'Num': bs_error_count, 'URL': url_addr, 'Title': "bs4_Error", 'Mailnum': 0, 'Email': 'None', 'Linknum': 0, 'Links': 'None', 'LocalLinks': 'None', 'ExtLinks': 'None', 'ErrorInfo': error})
                                mongodb_bs4_results_import(bs4_results_dict, error_flag) ### FAILED URL CRAWL TRACKING
                                print(' ')

                            time.sleep(1)
                except Exception as err:
                    print(red(f'Error occured during iteration throught the list of URLS'))
                    print(yellow("Error details: ") + red(err))
                    traceback.print_exception(type(err), err, err.__traceback__)
                    error_flag = True
        else:
            print(yellow(dt_print()) + red("  ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
            print(red("ERROR --- Query result atypical."))
            print(yellow("Error code: ") + red(query))

        total_urls = int(skipped_url_count) + int(successful_crawl_count) + int(bs_error_count)
        print(cyan(dt_print()) + yellow(" ||  = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
        print(cyan("From " + str(total_urls) + " URLs, ") + (red(str(bs_error_count))) + red(" failed, ") + yellow(str(skipped_url_count)) + yellow(" skipped and ") + green(str(successful_crawl_count)) + green(" successfully crawled."))
        mongodb_completed_query_copy(fetched_query)
        mongodb_query_delete(query_delete)
        time.sleep(2.0)
        countdown(wait_time)


if __name__ == "__main__":
        main()