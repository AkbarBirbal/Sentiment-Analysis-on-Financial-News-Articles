# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 13:29:33 2016

@author: bhavyateja
"""

import requests
import itertools
import urllib.request
import itertools
from multiprocessing import Pool
import http.client
from pymongo import MongoClient
from lxml import etree
import pickle

def extract_dump(url):
    data = urllib.request.urlopen(url[1]).read().decode("utf-8",errors='ignore')
    tree = etree.HTML(data)
    date =tree.xpath( "//meta[@name='DC.date.issued']" )[0].get("content")
    keywords = tree.xpath("//meta[@name='keywords']" )[0].get("content")
    data = tree.xpath("//*[@id='article-block']/div/p/text()")
    data = "".join(data)
    client = MongoClient()
    db = client.articles
    coll = db.business_line
    coll.insert_one({"date":date,"keywords":keywords,"article":data,"company_name":url[0]})
    client.close()
if __name__ == '__main__':
    file = open('articles_urls','rb')
    url = pickle.load(file)
    pool = Pool(4)
    pool.map(extract_dump,url)
    client = MongoClient()
    db = client.articles
    coll = db.business_line
    print(coll.count())
    