# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 18:43:23 2016

@author: bpotinen
"""

import urllib.request
import pickle
from multiprocessing import Pool
from lxml import etree
import itertools
import sys
import http.client
from pymongo import MongoClient
import requests
import re
import difflib
import csv

def extract_dump(url):
    data = urllib.request.urlopen(url[1]).read().decode("utf-8",errors='ignore')
    tree = etree.HTML(data)
    date =tree.xpath( "//meta[@name='DC.date.issued']" )[0].get("content")
    keywords = tree.xpath("//meta[@name='keywords']" )[0].get("content")
    data = tree.xpath("//*[@id='article-block']/div/p/text()")
    data = "".join(data)
    client = MongoClient()
    db = client.articles
    coll = db.business_line_1
    coll.insert_one({"date":date,"keywords":keywords,"article":data,"company_name":url[0]})
    client.close()
    




def extract_article_urls(url):
    li=[]
    data = urllib.request.urlopen(url).read().decode("utf-8",errors='ignore')
    tree = etree.HTML(data)
    f=tree.xpath('//*[@id="left-column"]/div/div/div/h2/a')
    f1 = tree.xpath('//*[@id="left-column"]/h3[1]/span')
    for i in f:
        li.append([f1[0].text,i.attrib.get('href')])
    return li





def extract_pages(url):
    q = '/?pageNo='
    u = url
    all_pages=[]
    tt = etree.HTML(urllib.request.urlopen(u).read().decode("utf-8",errors='ignore'))
    f1 = tt.xpath("//*[@id='left-column']/a/@href")
    first_pages =list(set(f1))
    if first_pages:
        first_pages.sort()
        last_p = first_pages[len(first_pages)-1]
        #print (u)
        #sys.stdout.flush()
        last_n = int(last_p.replace(u+q,""))
        for j in range(1,last_n+1):
            all_pages.append(u+q+str(j))
        return(all_pages)
if __name__ == '__main__':
    
    com_dict={}
    with open('companies_id_businessline.csv') as data_file:
        reader = csv.reader(data_file)
        for row in reader:
            if row[0]!='name':
                com_dict[row[0]]=row[1]
    names=[]
    f = open('companies.txt','r')
    for line in f:
        names.append(line.strip())
    found=[]
    not_found=[]
    for i in names:
        result=difflib.get_close_matches(i,com_dict.keys())
        if not result:
            not_found.append(i)
        else:
            for i in result:
                found.append([i,com_dict[i]])
                
    for i in not_found:
        for key,value in com_dict.items():
            if key.find(i)!=-1:
                found.append([key,com_dict[key]])
                not_found.remove(i)    
    keys=[]
    for key,value in com_dict.items():
        keys.append(key)
    for i in not_found:
        result=difflib.get_close_matches(i,com_dict.keys())
        if result:
            found.append([result[0],com_dict[result[0]]])
            not_found.remove(i)
    for i in not_found:
    #ii = i
        jj = i.split()
        for key,value in com_dict.items():
            if jj[0] in key:
                found.append([key,com_dict[key]])
    


    url_ready=[]
    for i in found:
        i[0]=i[0].replace(" ","-")
        url_ready.append(i)

    urls=[]
    base = 'http://www.thehindubusinessline.com/companies/tag/'
    for i in url_ready:
        urls.append(base+i[0]+'/'+i[1])
    new=[]
    for i in urls:
        if '&' in i:
            j = i.replace("-&-","-")
            urls.remove(i)
            new.append(j)
    for i in urls:
        if "(" in i:
            j=i.replace("(","")
            j = j.replace(")","")
            urls.remove(i)
            new.append(j)
    print(len(new))
    print(len(urls))
    a = urls+new
    for i in a:
        if '&' in i:
            a.remove(i)
        elif "(" in i:
            a.remove(i)
    print("read file got",len(a),"urls")
    pool = Pool(16)
    all_p = pool.map(extract_pages,a)
    all_p=[x for x in all_p if x is not None]
    all_all_p = list(itertools.chain.from_iterable(all_p))
    print("extracted all page navigations summed up to",len(all_all_p))
    filehandler = open("all_urls","wb")
    pickle.dump(all_all_p,filehandler)
    filehandler.close()
    urls=all_all_p
    all_p_2 = pool.map(extract_article_urls,urls)
    all_p_2=[x for x in all_p_2 if x is not None]
    all_all_p_2= list(itertools.chain.from_iterable(all_p_2))
    pattern1 = re.compile("Limited", re.IGNORECASE)
    pattern2 = re.compile("Ltd.", re.IGNORECASE)
    for i in all_all_p_2:
        a = i[0]
        c= pattern1.sub("",a)
        d = pattern2.sub("",c)
        i[0]=d.strip().lower()
    pool.map(extract_dump,all_all_p_2)
    
    
    
    
    

