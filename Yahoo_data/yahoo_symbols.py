from yahoo_finance import Share
from urllib.parse import quote
import json
import urllib.request
import pickle


f = open('companies.txt','r')
com = []
for i in f:
    com.append(i.strip())
query_1 = 'http://d.yimg.com/aq/autoc?query='
query_2 = '&region=US&lang=en-US&callback=YAHOO.util.ScriptNodeDataSource.callbacks'

quries =[]
for i in com:
    quries.append(query_1+quote(i)+query_2)
	
symb={}
not_found=[]
for i in quries:
    data = urllib.request.urlopen(i).read().decode("utf-8",errors='ignore')
    if data:
        ch = data.split("({")
        d = json.loads('{'+ch[1].replace(");",""))
        if d['ResultSet']['Result']:
            name = d['ResultSet']['Result'][0]['name']
            sym = d['ResultSet']['Result'][0]['symbol']
            symb[name] = sym
        else:
            not_found.append(i)
    else:
        not_found.append(i)
		
with open('symols', 'w') as f:
    json.dump(symb, f)
	
