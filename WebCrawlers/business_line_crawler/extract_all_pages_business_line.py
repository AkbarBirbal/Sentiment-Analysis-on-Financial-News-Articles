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
    urls = pickle.load(open('final','rb'))
    urls = list(set(urls))
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

