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
        r = requests.get(url, headers=headers, timeout=10)

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
    except Exception as ex:
        print(str(ex))
    finally:
        return hrefs

def get_listing(url):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
    html = None
    links = None
    hrefs = []
    sourceDestination = {"results":[]}
    try:
        r = requests.get(url, headers=headers, timeout=10)

        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            for a in soup.find_all('a', href=True):
                # Does the href start with a domain?
                if a['href'].startswith('/'):
                    hrefs.append(url + a['href'])
                    sourceDestination["results"].append({url:url + a['href']})
                elif a['href'] == './':
                    continue
                elif a['href'].startswith('./'):
                    concat = a['href'].replace('.', url)
                    hrefs.append(concat)
                    sourceDestination["results"].append({url:concat})
                else:
                    hrefs.append(a['href'])
                    sourceDestination["results"].append({url:a['href']})
#            listing_section = soup.select('#offers_table table > tbody > tr > td > h3 > a')
#            links = [link['href'].strip() for link in listing_section]
    except Exception as ex:
        print(str(ex))
    finally:
        return hrefs, sourceDestination


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


def parseUrl(url):
    
    parsed = urisplit(url)
    return parsed

#car_links = None
#cars_info = []
#cars_links = get_listing('https://www.olx.com.pk/cars/')
#
#[cars_info.append(parse(car_link)) for car_link in cars_links]
#if len(cars_info) > 0:
#    with open('data.csv', 'a+') as f:
#        f.write('\n'.join(cars_info))
        
if __name__ == "__main__":
    
#    car_links = None
#    cars_info = []
#    cars_links = get_listing('https://www.olx.com.pk/cars/')

    hrefs, sourceDestination = get_listing('https://www.google.com')
    sourceDestinationL = []
    
    start = time()
    p = Pool(3)
    records, sourceDestination = p.map(get_listing, hrefs)
    p.terminate()
    p.join()
    
    totaltime = time()-start
    
    print(totaltime)
    
    for i in sourceDestination['results']:
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
    
    flatlist = [item for sublist in records for item in sublist]
    
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