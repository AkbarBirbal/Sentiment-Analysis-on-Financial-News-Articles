import urllib.request
import json
import csv

fi = open('symols','r')
data = json.load(fi)

burl = 'http://ichart.finance.yahoo.com/table.csv?s='

val =[]
for keys,vals in data.items():
    val.append([keys,vals])	
	
	
	
output_path = "D:\\data"
write_path ="D:\\data\\updated"


not_found=[]
for i in val:
    url = burl+i[1]
    output_path_company=output_path+'\\'+i[0]+'.csv'
    try:
        urllib.request.urlretrieve(url,output_path_company)
        with open(output_path_company,'r') as csvinput:
            with open(write_path+'\\'+i[0]+'.csv', 'w') as csvoutput:
                writer = csv.writer(csvoutput, lineterminator='\n')
                reader = csv.reader(csvinput)
                all = []
                row = next(reader)
                row.append('company')
                all.append(row)
                for row in reader:
                    row.append(i[0])
                    all.append(row)
                writer.writerows(all)  
    except:
        not_found.append(i)
        pass
mer = []
for i in val:
    if i not in not_found:
        mer.append(i)
		
touch_path = "D:\\data\\updated\\"
fout=open("merge.csv","a")
for i in mer:
    if i == mer[0]:
        for line in open(touch_path+i[0]+".csv"):
            fout.write(line)
    else:
        f = open(touch_path+i[0]+".csv")
        next(f)
        for line in f:
            fout.write(line)
        f.close()
    
