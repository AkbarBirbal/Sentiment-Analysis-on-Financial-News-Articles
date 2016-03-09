# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 04:44:10 2015

@author: bhavyateja
"""
import pymongo
import urllib.request
from parsel import Selector
from pymongo import MongoClient
import urllib.error
import progressbar
from socket import timeout
client = MongoClient()
db = client.articles
coll = db.dataset
url=[]
myfile = open('businessweek_1.txt',"r")
urls = myfile.readlines()
for a in urls:
    url.append(a.strip())
s_a = set(url)
s = list(s_a)
fail = []
bar = progressbar.ProgressBar() 
for i in bar(range(7995,len(s))):
    flag = False
    for _ in range(5):
        try:
            response=urllib.request.urlopen(s[i])
            break
        except urllib.error.URLError:
            pass
        except urllib.error.HTTPError:
            pass
        except timeout:
            pass
    else:
        fail.append(s[i]) 
        print ("failed to retive info from ",s[i],i)
        flag = True
    if flag ==True:
        pass
    else:
        clap = response.read()
        clap = clap.decode("utf-8") 
        h = Selector(text=clap)
        date = h.xpath('//meta[@content][@name="pub_date"]/@content').extract()
        if date:
            pass
        else:
            date = h.xpath('//meta[@content][@name="parsely-pub-date"]/@content').extract()
        key = h.xpath('//meta[@content][@name="keywords"]/@content').extract() 
        info = h.xpath('//div[@id = "article_body"]/p//text()').extract()
        if not info:
            info = h.xpath('//div[@class = "article-body__content"]/p//text()').extract()
        if len(info)>1:
            info = ' '.join(str(r) for r in info)
            info = info.replace(u"\xa0", u" ")
        if "T" in date[0]:
            date,t = date[0].split('T')
        else:
            date = date[0]
        coll.insert_one({"date":date,"keywords":key,"artical":info})
     
    
