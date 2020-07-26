#mongoSpade_beautifulsoup_scrape.py

import re
from bs4 import BeautifulSoup
from urllib.error import HTTPError




def beautifulsoup_scrape(url):
    error_count = 0
    error_queries = []
    try:
        hdr = {'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36"}
        req = Request(url,headers=hdr)
        page = urlopen(req, timeout = 5)
        html = page.read()
        soup = BeautifulSoup(html.decode('utf-8', 'ignore'), "html.parser")
        title_text = str(soup.title.text)
        found_mail = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', soup.text)
        bs4_result = ([title_text, found_mail, "!!!no_errors!!!"])
        return bs4_result

    except Exception as error:
        title_error = "BeautifulSoup error"
        mail_error = "BeautifulSoup error"
        error_notif = str(error)
        error_return = ([title_error, mail_error, "!!!ERROR!!!"])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", "-u", help="--url [url of site for crawling]")
    args = parser.parse_args()
    if args.url:
        url = args.url
    print(green("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ="))
    print(green(f'Crawling url: {yellow(url)}'))
    beautifulsoup_scrape(url)