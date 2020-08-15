#mongoSpade_beautifulsoup_scrape.py

import argparse
import re
import time
import typing
import textwrap
from bs4 import BeautifulSoup
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from components.mongoSpade_stdout import *
from components.config_skip import *


def beautifulsoup_scrape(url):
    error_count = 0
    error_queries = []
    url_parsed = urlparse(url)
    urlparse_host = url_parsed.hostname
    urlparse_path = url_parsed.path
    urlparse_pagequery = url_parsed.query
    domain_skip = any(skip_host in urlparse_host for skip_host in skip_hosts)
    ext_skip = any(skip_ext in urlparse_path for skip_ext in skip_ext)
    if domain_skip is not True and ext_skip is not True:
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
            for link in soup.findAll('a', attrs={'href': re.compile("^https://")}):
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
        if domain_skip is True:
            bs4_result = (["Domain found in skip_hosts", "SKIPPED", "!!!SKIPPED!!!", urlparse_host])
        elif ext_skip is True:
            bs4_result = (["Extension found in skip_ext", "SKIPPED", "!!!SKIPPED!!!", urlparse_path])
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