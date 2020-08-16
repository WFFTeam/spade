#mongoSpade_link_recon.py

import urllib    
from bs4 import BeautifulSoup

def bs_link_recon(url):
    resp = urllib.request.urlopen(url)
    # Get server encoding per recommendation of Martijn Pieters.
    soup = BeautifulSoup(resp, from_encoding=resp.info().get_param('charset'), features="lxml")  
    links = []
    for line in soup.find_all('a'):
        link = line.get('href')
        if not link:
            continue
        elif link:
            links.append(link)
        
    # Depending on usage, full internal links may be preferred.
    # full_internal_links = {
    #     urllib.parse.urljoin(url, internal_link) 
    #     for internal_link in internal_links
    # }

    # # Print all unique external and full internal links.
    # for link in external_links.union(full_internal_links):
    #     print(link)

    # for line in links:
    #     print(line)
    #print(links)
    return links