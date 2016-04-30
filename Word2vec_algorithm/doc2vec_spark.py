import math
import numpy as np
from operator import add
import logging 
from pyspark import SparkContext
import sys

def unigram(li):
	power = 0.75
	norm = sum([math.pow(t[1], power) for t in li]) 
	table_size = 1e8
	table = np.zeros(table_size)
	p = 0
	i = 0 
	old_i = 0 
	for j, unigram in enumerate(li):
		p += float(math.pow(unigram[1], power))/norm
		while i < table_size and float(i) / table_size < p:
			table[i] = j
			i += 1
		old_i = i - old_i
		sys.stdout.write("\r propability for index '%d' is %f, kept it	%d times" %(unigram[1],p,old_i))
	return table
	
def mapf(sen):
    dum=[]
    words=sen.split()
    for i in words:
            if i in vocab_hash:
                    dum.append(vocab_hash[i])
    return(dum)

	
def sigmoid(z): 
	if z > 6:
		return 1.0
	elif z < -6:
		return 0.0
	else:
		return 1 / (1 + math.exp(-z))

def mapfunction(chunk):
	lin = gin.value
	lw = gw.value
	tab = btc.value
	vs = vsb.value
	ldv = dv.value
	input_track = []
	weight_track = []
	ldv_track=[]
	updated=[]
	data =[]
	ds = tdc.value
	current_word_count=0  
	alpha_count = 0
	last_alpha_count = 0
	for i in chunk:
		data.append(i)
	c_s = len(data)
	wc = 0
	for sen,indx in data:
		wc = wc+len(sen)
	
	starting_alpha=0.025
	input_track = np.zeros(shape=(vs))
	weight_track = np.zeros(shape=(vs))
	ldv_track = np.zeros(shape=(ds))
	for sentence,doc_index in data:
		ldv_track[doc_index]=ldv_track[doc_index]+1
		for sent_pos, token in enumerate(sentence):
			neu1e = np.zeros(100)
			#sys.stdout.write("\r running word count  %d, remaining words %d" %(word_count.value,total_words.value-word_count.value))
			#sys.stdout.flush()
			if current_word_count % 10000 == 0:
				alpha_count += (current_word_count - last_alpha_count)
				last_alpha_count = current_word_count
				alpha = starting_alpha * (1 - float(alpha_count) / wc)
				sys.stdout.write("\r current_word_count %d alpha %f remaining %d" %(current_word_count,alpha,(wc-current_word_count)))
				sys.stdout.flush()
				if alpha < starting_alpha * 0.0001: 
					alpha = starting_alpha * 0.0001
			current_win = np.random.randint(low=1, high=5+1)
			context_start = max(sent_pos - current_win, 0)
			context_end = min(sent_pos + current_win + 1,wc)
			context = sentence[context_start:sent_pos] + sentence[sent_pos+1:context_end]
			for context_word in context:
				input_track[context_word]=input_track[context_word]+1
				neu1e = np.zeros(100)
				classifiers = [(token, 1)] + [(target, 0) for target in np.random.choice(tab,2)]
				for target, label in classifiers:
					weight_track[target]=weight_track[target]+1
					z = np.dot(lin[context_word],lw[target])
					if z>6:
						p=1.0
					elif z<6:
						p = 0.0
					else:
						p=1/(1+math.exp(-z))
				
					g = alpha * (label - p)
					neu1e+=g*lw[target]
					lw[target] +=g* lin[context_word]
				lin[context_word] +=neu1e
				ldv[doc_index]+=neu1e
			current_word_count+=1
			#word_count.add(1)
	for indd,val in enumerate(input_track):
		if val>0:
			updated.append(['input',indd,lin[indd]])
	for indd,val in enumerate(weight_track):
		if val>0:
			updated.append(['weight',indd,lw[indd]])
	for indd,val in enumerate(ldv_track):
		if val>0:
			updated.append(['doc_vec',indd,ldv[indd]])
	return iter(updated)

def save(li,doc_dict, syn0,doc_vec, fo):
	fo = open(fo, 'w')
	dim = len(syn0[0])
	fo.write('%d %d\n' % (len(syn0), dim))
	for token, vector in zip(li, syn0):
		word = token[0]
		vector_str = ' '.join([str(s) for s in vector])
		fo.write('%s %s\n' % (word, vector_str))
	fo.write('%d %d\n' % (len(doc_vec),dim))
	for token, vector in enumerate(doc_vec):
		label = doc_dict[token]
		vector_str = ' '.join([str(s) for s in vector])
		fo.write('%s %s\n' % (label, vector_str))
	fo.close()


if __name__ == "__main__":
	sc = SparkContext(appName="word2vec")
	pos_data = sc.textFile('<possitive_text>')
	neg_data = sc.textFile('<negitive_text>')
	data = pos_data.union(neg_data)
	
	#build vocab
	inp = data.map(lambda row: row.split(" "))
	words = inp.flatMap(lambda x: x)
	vocab = words.map(lambda x: (x,1)).reduceByKey(add).filter(lambda (k,v):v>=5)
	print('vocab size',vocab.count())
	li = vocab.sortBy(lambda (k,v):v,ascending=False).collect()
	vocab_hash ={}
	for i,word in enumerate(li):
		vocab_hash[word[0]]=i
	
	#build_unigram_table
	table = unigram(li)
	
	#convert words to index
	pos_ind = pos_data.map(mapf)
	neg_ind = neg_data.map(mapf)
	
	#lable_sentences_possitive_and_negitive
	pos_label = pos_ind.zipWithIndex().map(lambda (sen,indx):('pos_'+str(indx),sen))
	neg_label = neg_ind.zipWithIndex().map(lambda (sen,indx):('neg_'+str(indx),sen))
	total_label_data = pos_label.union(neg_label).keys()
	td = total_label_data.collect()
	doc_dict = {}
	for i,label in enumerate(td):
		doc_dict[i] = label
	#club_both_datasets
	train_data = pos_ind.union(neg_ind)
	
	#map_every_sen_with_index_as_a_reference_to_vector_in_matrix
	train_data= train_data.zipWithIndex()
	
	btc = sc.broadcast(table)
	train_data.cache()
	total_doc = train_data.count()
	vocab_size=len(li)
	vsb = sc.broadcast(vocab_size)
	tdc = sc.broadcast(total_doc)
	input_word = np.random.uniform(low=-0.5/100, high=0.5/100, size=(vocab_size, 100))
	weights = np.zeros(shape=(vocab_size, 100))
	doc_vec = np.random.uniform(low=-0.5/100, high=0.5/100, size=(total_doc, 100))
	train_data = train_data.collect()
	rd_data = sc.parallelize(train_data,10)
	for i in range(2):
		print ("in iteration",i)
		
		gin = sc.broadcast(input_word)
		gw = sc.broadcast(weights)
		dv = sc.broadcast(doc_vec)
		upda = rd_data.mapPartitions(mapfunction)
		upda = upda.cache()
		upda.count()
		up_input = upda.filter(lambda (k1,k2,v1): k1 == 'input')
		up_inp = up_input.map(lambda (k1,k2,v1):(k2,v1))
		up_1 = up_inp.reduceByKey(add).sortByKey().map(lambda (k,v):v)
		up =up_1.collect()
		print("returned input_word matrix count ",len(up))
		gin.unpersist(False)	
		gw.unpersist(False)
		input_word = np.vstack(up)
		up_wight = upda.filter(lambda (k1,k2,v1): k1 == 'weight')
		up_wig = up_wight.map(lambda (k1,k2,v1):(k2,v1)).reduceByKey(add).sortByKey().map(lambda (k,v):v)
		up_2=up_wig.collect()
		print("returned weight matrix count  ",len(up_2))
		weights = np.vstack(up_2)
		up_doc = upda.filter(lambda (k1,k2,v1): k1 == 'doc_vec')
		s_doc = up_doc.map(lambda (k1,k2,v1):(k2,v1)).reduceByKey(add).sortByKey().map(lambda (k,v):v)
		up_3=s_doc.collect()
		doc_vec = np.vstack(up_3)
	save(li,doc_dict,input_word,doc_vec,'<output>')
	sc.stop()


