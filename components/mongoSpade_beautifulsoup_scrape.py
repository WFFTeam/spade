#mongoSpade_beautifulsoup_scrape.py

import argparse
from termcolor import colored
from datetime import datetime as dt
import re
import time
from bs4 import BeautifulSoup
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from components.config_skip import *

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

def beautifulsoup_scrape(url):
    error_count = 0
    error_queries = []
    url_parsed = urlparse(url)
    urlparse_host = url_parsed.hostname
    urlparse_path = url_parsed.path
    urlparse_pagequery = url_parsed.query

   #print(yellow("Domain: ") + cyan(urlparse_host)) ###DEBUG
    url_in_skiphosts = any(skip_host in urlparse_host for skip_host in skip_hosts)
   #if urlparse_host not in skip_hosts:
   #print(cyan(url_in_skiphosts))
    if url_in_skiphosts is not True:
        try:
            time.sleep(0.2)
            hdr = {'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36"}
            req = Request(url,headers=hdr)
            page = urlopen(req, timeout = 5)
            html = page.read()
            soup = BeautifulSoup(html.decode('utf-8', 'ignore'), "html.parser")
            link_list = []
            title_text = str(soup.title.text)
            found_mail = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', soup.text)
            for link in soup.findAll('a', attrs={'href': re.compile("^http://")}):
                link_list.append(link.get('href'))
            bs4_result = ([title_text, found_mail, link_list, urlparse_host])
            return bs4_result

        except Exception as e:
            title_error = "BeautifulSoup error"
            mail_error = "BeautifulSoup error"
            error_notif = str(e)
            error_return = ([title_error, error_notif, "!!!ERROR!!!", urlparse_host])
            return error_return
    else:
        bs4_result = (["Domain found in skip_hosts", "SKIPPED", "!!!SKIPPED!!!", urlparse_host])
        return bs4_result

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