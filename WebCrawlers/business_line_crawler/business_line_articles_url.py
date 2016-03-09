# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 12:22:21 2016

@author: bpotinen
"""

import pickle
from lxml import etree
import urllib.request
from multiprocessing import Pool
import itertools
def extract_article_urls(url):
    li=[]
    data = urllib.request.urlopen(url).read().decode("utf-8",errors='ignore')
    tree = etree.HTML(data)
    f=tree.xpath('//*[@id="left-column"]/div/div/div/h2/a')
    f1 = tree.xpath('//*[@id="left-column"]/h3[1]/span')
    for i in f:
        li.append([f1[0].text,i.attrib.get('href')])
    return li

if __name__ == '__main__':
    file = open('all_urls','rb')
    urls = pickle.load(file)
    print("read file got",len(urls),"urls")
    pool = Pool(4)
    all_p = pool.map(extract_article_urls,urls)
    all_p=[x for x in all_p if x is not None]
    all_all_p = list(itertools.chain.from_iterable(all_p))
    print("extracted all the articles summed up to",len(all_all_p))
    filehandler = open("articles_urls","wb")
    pickle.dump(all_all_p,filehandler)
    filehandler.close()