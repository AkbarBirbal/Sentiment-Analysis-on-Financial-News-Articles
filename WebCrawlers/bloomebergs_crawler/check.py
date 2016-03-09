import urllib.request
from parsel import Selector
import socket
from multiprocessing import Pool
import itertools
def mon(inputs):
    week=[]
    errored_out=[]
    for month in inputs:
        try:data = urllib.request.urlopen(month).read()
        except urllib.error.URLError as e:
            print (month)
            errored_out.append(month)
            print(e.reason)
        if type(data) is bytes:
            data = data.decode("utf-8")      
            hxs = Selector(text=data)
            weeks = hxs.xpath('//ul[@class="weeks"]/li/a').re('http://www.businessweek.com/archive/\\d+-\\d+/news/day\\d+\.html')
            week.append(weeks)
        else:
            hxs = Selector(text=data)
            weeks = hxs.xpath('//ul[@class="weeks"]/li/a').re('http://www.businessweek.com/archive/\\d+-\\d+/news/day\\d+\.html')
            week.append(weeks)
    return week
def post(inputs):
    posted=[]
    failed=[]
    for week in inputs:
        try:data = urllib.request.urlopen(week).read()
        except urllib.error.URLError as e:
            failed.append(week)
            print(week)
            print(e.reason) 
        if type(data) is bytes:
            data = data.decode("utf-8") 
            hxs = Selector(text=data)
            posts = hxs.xpath('//ul[@class="archive"]/li/span[@class="channel markets_and_finance"]/following-sibling::h1/a/@href').extract()
            posted.append(posts)
        else:
            hxs = Selector(text=data)
            posts = hxs.xpath('//ul[@class="archive"]/li/span[@class="channel markets_and_finance"]/following-sibling::h1/a/@href').extract()
            posted.append(posts)
    return posted
if __name__ == '__main__':
    print("in main")
    totalWeeks = []
    totalPosts = []
    url = 'http://www.businessweek.com/archive/news.html#r=404'
    data = urllib.request.urlopen(url).read()
    data = data.decode("utf-8") 
    sel = Selector(text=data)
    months = sel.xpath('//ul/li/a').re('http://www.businessweek.com/archive/\\d+-\\d+/news.html')
    #admittMonths = 12*(2015-1991) + 8
    m=[]
    for i in months:
        m.append([i])
    totalWeeks = []
    pool = Pool(8)
    totalWeeks= pool.map(mon,m)
    totalWeeks = [ent for sublist in totalWeeks for ent in sublist]
    print (len(totalWeeks))
    #club = [ent for sublist in totalWeeks for ent in sublist]
    #print (len(club))
    club = [ent for sublist in totalWeeks for ent in sublist]
    print (len(club))
    d=[]
    for i in club:
        d.append([i])
    print (len(d))
    posts=[]
    pool.close()
    pool = Pool(8)
    posts=pool.map(post,d)
    print (len(posts))
    mearg = list(itertools.chain.from_iterable(posts))
    m = [x for x in mearg if x != []]
    total = list(itertools.chain.from_iterable(m))
    print (len(total))
    
    
    