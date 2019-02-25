# -*- coding: utf-8 -*-
"""
Created on Tue Dec  4 09:24:57 2018

@author: Nate
"""
"""
Visualize routing with D3
Write JSON with status attribution collected during scan
Web demand model
"""

import requests
from bs4 import BeautifulSoup
from time import sleep, time
from multiprocessing import Pool
from pandas import DataFrame
import csv
from uritools import urisplit
import pandas as pd
import re
import json


def scrape_one(url):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
    html = None
    links = None
    hrefs = []
#    sourceDestination = {"results":[]}
    try:
        r = requests.get(url, headers=headers, timeout=30)

        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            for a in soup.find_all('a', href=True):
                # Does the href start with a domain?
                if a['href'].startswith('/'):
                    hrefs.append(url + a['href'])
#                    sourceDestination["results"].append({url:url + a['href']})
                elif a['href'] == './':
                    continue
                elif a['href'].startswith('./'):
                    concat = a['href'].replace('.', url)
                    hrefs.append(concat)
#                    sourceDestination["results"].append({url:concat})
                else:
                    hrefs.append(a['href'])
#                    sourceDestination["results"].append({url:a['href']})
#            listing_section = soup.select('#offers_table table > tbody > tr > td > h3 > a')
#            links = [link['href'].strip() for link in listing_section]
    except requests.exceptions.Timeout as e:
        print (e)
    except Exception as ex:
        print(str(ex))
    finally:
        return hrefs

def get_links(url):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
    html = None
    links = None
#    hrefs = []
    sourceDestination = {"results":[]}
    try:
        r = requests.get(url, headers=headers, timeout=30)

        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            root = Parse(url).scheme + '://' + Parse(url).authority
            for a in soup.find_all('a', href=True):
                # Does the href start with a domain?
                # Reference:
                # / = root of the current drive
                # ./ = current directory
                # ../ = parent of the current directory
                
                if a['href'].startswith('/'):
                    link = root + a['href']
#                    sourceDestination["results"].append({root: link})
                elif a['href'] == './' or a['href'] == '#':
                    continue
                elif a['href'].startswith('./'):
                    link = a['href'].replace('.', url)
                    root = url
#                    sourceDestination["results"].append({url: link})
                else:
                    link = a['href']
                    root = url
#                    sourceDestination["results"].append({url: link})
                    
                sourceDestination["results"].append({root: link})
    except requests.exceptions.ConnectionError:
        pass
    except Exception as ex:
        print(str(ex))
    finally:
        return sourceDestination


# parse a single item to get information
def parse(url):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}

    info = []
    title_text = '-'
    location_text = '-'
    price_text = '-'
    title_text = '-'
    images = '-'

    try:
        r = requests.get(url, headers=headers, timeout=10)
        sleep(2)

        if r.status_code == 200:
            print('Processing..' + url)
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            title = soup.find('h1')
            if title is not None:
                title_text = title.text.strip()

            location = soup.find('strong', {'class': 'c2b small'})
            if location is not None:
                location_text = location.text.strip()

            price = soup.select('div > .xxxx-large')
            if price is not None:
                price_text = price[0].text.strip('Rs').replace(',', '').replace('\n', '')

            images = soup.select('#bigGallery > li > a')
            img = [image['href'].replace('\n', '').strip() for image in images]
            images = '^'.join(img)

            info.append(url)
            info.append(title_text)
            info.append(location_text)
            info.append(price_text)
            info.append(images)
    except Exception as ex:
        print(str(ex))
    finally:
        if len(info) > 0:
            return ','.join(info)
        else:
            return None

class Parse:
    
    def __init__(self, url):
        self.parse = urisplit(url)
        self.scheme = self.parse.scheme
        self.authority = self.parse.authority
        self.path = self.parse.path
        self.query = self.parse.query
        self.fragment = self.parse.fragment
        
def parseUrl(url):
    
    parsed = urisplit(url)
    return parsed
        
if __name__ == "__main__":
    
#    hrefs = scrape_one('https://www.google.com')
    # Seed links
    sourceDestination = get_links('https://www.google.com')
    
    # Produce second level links    
    for i in sourceDestination['results']:
        for j in i:
            for k,v in i.items():
                if k == v:
#                    print("Same value in key and value!")
                    continue
                elif v == '' or v == None:
                    continue
                # AttributeError: 'list' object has no attribute 'startswith'
                elif not v.startswith('http') or not v.startswith('https'):
                    continue
                else:
#                    print("Getting links from " + str(v))
                    newLink = get_links(v)
                    for link in newLink['results']:
                        sourceDestination['results'].append(link)
#                    if newLink == '' or newLink == None:
#                        continue
#                    print(newLink)
#                    sourceDestination['results'].append(newLink)
#                    print("Complete!")

    sourceDestinationL = []
    
#    start = time()
#    p = Pool(3)
#    records, sourceDestination = p.map(get_links, hrefs)
#    p.terminate()
#    p.join()
#    
#    totaltime = time()-start
#    
#    print(totaltime)
    
    for i in sourceDestination['results']:
        for j in i:
            for k, v in i.items():
                sourceDestinationL.append([k,v])
            
    sdDf = DataFrame(sourceDestinationL, columns=['Source', 'Destination'])
    
    sdDf.rename(columns={"Source":"source","Destination":"target"}, inplace=True)
    sdDf
    grouped_src_dst = sdDf.groupby(["source","target"]).size().reset_index()
    grouped_src_dst
    unique_urls = pd.Index(grouped_src_dst['source']
                      .append(grouped_src_dst['target'])
                      .reset_index(drop=True).unique())
    unique_urls
    grouped_src_dst.rename(columns={0:'count'}, inplace=True)
    temp_links_list = list(grouped_src_dst.apply(lambda row: {"source": row['source'], "target": row['target'], "value": row['count']}, axis=1))
    
    links_list = []
    for link in temp_links_list:
        record = {"value":link['value'], "source":unique_urls.get_loc(link['source']), "target": unique_urls.get_loc(link['target'])}
        links_list.append(record)
    links_list
    temp_links_list
    nodes_list = []
    count = 1
            
    for url in unique_urls:
        nodes_list.append({"id":url, "group":count})
        count += 1
    nodes_list 
    json_prep = {"nodes":nodes_list, "links": links_list}
#    json_prep.keys()
    json_prep
    json_dump = json.dumps(json_prep, indent=1, sort_keys=True)
    json_dump
    filename_out = r'C:\Users\nate\Documents\Programming\Javascript\d3\force-directed-graph\data\googlelinks.json'
    json_out = open(filename_out,'w')
    json_out.write(json_dump)
    json_out.close()
    
#    flatlist = [item for sublist in records for item in sublist]
    
    df = DataFrame(flatlist)
    df.drop_duplicates(inplace=True)
    df = df.rename(index=str, columns={0:'url'})

    urls = df['url'].tolist()
    
    for url in urls:
        hrefs.append(url)
        
#    len(hrefs)
    domains = []
    gDomainsL = []
#    hrefs
    
    word = 'google'
    regexp = re.compile(r'google')
    
    for l in hrefs:
        parsed = parseUrl(l)
        if parsed.authority == None:
            continue
        domains.append(parsed.authority)
    
    for l in hrefs:
        parsed = parseUrl(l)
        if parsed.authority == None:
            continue
        if re.search(r'\bgoogle\b', parsed.authority):
            gDomainsL.append(parsed.authority)
        
#    domains
    
    df = pd.DataFrame(domains)
    df.drop_duplicates(inplace=True)
    df
        
    with open(r'C:\Users\nate\Documents\Data\webscraping\googledomains.csv', 'w') as outfile:
        wr = csv.writer(outfile)
        wr.writerow(gDomainsL)
        for row in zip(hrefs):
            row=(s.encode('utf-8') for s in row)
            if row != "" or row != None:
                wr.writerow(row)
    
    urlList = []            
    
    with open(r'C:\Users\nate\Documents\Data\webscraping\google.csv','r') as fin:
        reader=csv.reader(fin)
        for row in reader:
            try:
                urlList.append(row[0][row[0].find("b"):])
            except IndexError as e:
                continue

#        for i in reader:
#            print(row[1])
#            temp=list(row)
#            fmt=u'{:<15}'*len(temp)
#            print (fmt.format(*[s.decode('utf-8') for s in temp]))