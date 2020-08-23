#mongoSpade_beautifulsoup_scrape.py

import argparse
import re
import time
import typing
import textwrap
import traceback
import requests
from bs4 import BeautifulSoup
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from components.mongoSpade_stdout import *
from components.config_skip import *

def parse_url(url):
    parse_results = urlparse(url)
    parse_scheme = parse_results.scheme
    parse_host = parse_results.hostname
    parse_path = parse_results.path
    parse_query = parse_results.query
    parse_params = parse_results.params
    parse_frags = parse_results.fragment

    parse_url_dict = { 'scheme': parse_scheme, 'netlock': parse_host, 'path': parse_path, 'query': parse_query, 'params': parse_params, 'frags': parse_frags } 
    return parse_url_dict

def beautifulsoup_scrape(url):

    error_count = 0
    error_queries = []
    link_list = []

    url_host = urlparse(url).hostname
    url_path = urlparse(url).path
   
    domain_skip = any(skip_host in url_host for skip_host in skip_hosts)
    ext_skip = any(skip_ext in url_path for skip_ext in skip_ext)

    ### URLPARSE dictionary
    ############################################################################################################################################################
    # url_parsed_dict = {}
    # parse_dict = parse_url(url)
    # parse_scheme = parse_dict["scheme"]
    # parse_host = parse_dict["netlock"]
    # parse_path = parse_dict["path"]
    # parse_query = parse_dict["query"]
    # parse_params = parse_dict["params"]
    # parse_frags = parse_dict["frags"]
    # parse_url_dict = { 'scheme': parse_scheme, 'netlock': parse_host, 'path': parse_path, 'query': parse_query, 'params': parse_params, 'frags': parse_frags } 
    ############################################################################################################################################################
    
    if domain_skip is not True and ext_skip is not True:
        try:
            hdr = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
            }

            req = Request(url,headers=hdr)
            page = urlopen(req, timeout = 5)
            html = page.read() 

            time.sleep(0.2)
            soup = BeautifulSoup(html.decode('utf-8', 'ignore'), "lxml")
            
            title_text = str(soup.title.text)
            found_mail = list(set(re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', soup.text)))
            for link in soup.findAll('a', attrs={'href': re.compile("^http://")}):
                link_list.append(link.get('href'))
            link_list = list(set(link_list))
            bs4_result = ([title_text, found_mail, link_list, url_host])
            return bs4_result

        except Exception as err:
            mail_error = title_error = "BeautifulSoup error"
            error_notif = str(err)

            error_return = ([title_error, error_notif, "!!!ERROR!!!", url_host])
           #traceback.print_exception(type(err), err, err.__traceback__) ### DEBUG
            return error_return
    else:
        if domain_skip is True:
            bs4_result = (["Domain found in skip_hosts", "SKIPPED", "!!!SKIPPED!!!", url_host])
        elif ext_skip is True:
            bs4_result = (["Extension found in skip_ext", "SKIPPED", "!!!SKIPPED!!!", url_path])
        return bs4_result

### ALT BS SCRAPING FUNCTION --- TODO
################################################################################################
# def alt_bs4_scrape(url):
#     try:
#         error_count = 0
#         error_queries = []
#         link_list = []
#
#         url_host = urlparse(url).hostname
#         url_path = urlparse(url).path
#      
#         domain_skip = any(skip_host in url_host for skip_host in skip_hosts)
#         ext_skip = any(skip_ext in url_path for skip_ext in skip_ext)
#
#         html = requests.get(url)
#         soup = BeautifulSoup(html.decode('utf-8', 'ignore'), "lxml") ### OLD SOUP METHOD
#         soup = BeautifulSoup(html.text, "html.parser") 
#
#         title_text = str(soup.title.text)
#         found_mail = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', soup.text)
#         for link in soup.findAll('a', attrs={'href': re.compile("^http://")}):
#             link_list.append(link.get('href'))
#         link_list = list(set(link_list))
#         bs4_result = ([title_text, found_mail, link_list, url_host])
#         return bs4_result
#     except Exception as err:
#         mail_error = title_error = "BeautifulSoup error"
#         error_notif = str(err)
#         error_return = ([title_error, error_notif, "!!!ERROR!!!", url_host])
#         traceback.print_exception(type(err), err, err.__traceback__)
#         return error_return
################################################################################################

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", "-u", help="--url [url of site for crawling]")
    args = parser.parse_args()
    if args.url:
        url = args.url
    print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
    print(green(f'Crawling url: {yellow(url)}'))
    beautifulsoup_scrape(url)

if __name__ == "__main__":
    main()