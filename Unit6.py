import sys
reload(sys)
sys.setdefaultencoding('utf8')
import nltk
#from nltk.book import *
from nltk.probability import FreqDist
from nltk.corpus import stopwords
import json
import time
from multiprocessing import Pool
from collections import Counter
from nltk.stem.porter import PorterStemmer
import pickle
import gensim
from gensim import corpora, models
import numpy as np

input_file_path = "../Output_Files/big/cleaned.json"

def get_most_frequent_words():
	docfreq = pickle.load(open(picklefile, 'r'))
	max_f = docfreq.most_common(1)[0][1]
	print max_f
	stop_words = [k for k in docfreq if docfreq[k] > 0.5*max_f]
	print stop_words
	return stop_words

def get_stopwords():
	print "Retrieving list of stopwords..."
	nltk_defaults = stopwords.words('english')
	punctuation = [',', ':', '.', '...', ';', '-', '"', '--', '!', '?', '(', ')', '``', '\'\'']
	custom_words = ['would', 'though', 'it', 'still', 'he', 'at', 'even', 'but', 'like', 'upon', 'a', 'mr.']
	return set(nltk_defaults + punctuation + custom_words)

def get_input(fname):
	stop_words = get_stopwords()
	print "Reading Input JSON..."
	with open(fname) as f:
		content = f.readlines()
	content = [(json.loads(x.lower()), stop_words) for x in content]
	print "Done loading json!"
	return content

def test():
	documents = get_input(input_file_path)
	print documents


def evaluate(row):
	# clean data
	record, stop_words = row
	text = nltk.word_tokenize(record['text'].lower())
	p_stemmer = PorterStemmer()
	stemmed_stopwords = [p_stemmer.stem(i) for i in stop_words]
	stemmed_text = [p_stemmer.stem(i) for i in text]
	stopped_tokens = [i for i in stemmed_text if (i not in stemmed_stopwords and len(i) > 2)]
	return stopped_tokens


def main():
	st = time.time()
	print "Start Time: ", st
	documents = get_input(input_file_path)
	p = Pool(15)
	urls = [row[0]['title'] for row in documents]
	individual_results = p.map(evaluate, documents)
	dictionary = corpora.Dictionary(individual_results)
	corpus = [dictionary.doc2bow(text) for text in individual_results]
	tfidf = gensim.models.TfidfModel(corpus)
	imp_corpus = tfidf[corpus]
	lsimodel = models.LsiModel(imp_corpus, id2word = dictionary)
	cohmodel = models.CoherenceModel(model=lsimodel, corpus=imp_corpus, coherence='u_mass')
	print 'Coherence:', cohmodel.get_coherence_per_topic()
	lsi_corpus = lsimodel[imp_corpus]
	# Use the singular values to choose how many components to use
	v = lsimodel.projection.s**2 / sum(lsimodel.projection.s**2)
	print v[:100]
	k = np.argmin(v>0.005)+1	# Hard threshold, may be better to plot and find the knee
	topics = lsimodel.show_topics(num_topics=k, num_words=5)
	#topcis2 = ldamodel.get_topics()
	for i, topic in enumerate(topics):
		print topic
		tops = sorted(zip(range(len(lsi_corpus)), lsi_corpus), reverse=True, key=lambda doc: abs(dict(doc[1]).get(i, 0.0)))
    		print 'Most relevant documents: '
		for top in tops[:10]:
        		print urls[top[0]]
		print
	#print corpus[0]
	end = time.time()
	print "End Time: ", end-st

	

	#final_results = compile_results(individual_results)
	#print "Final Results", final_results

	#print "Final Results ayy lmao: "
	#for res in final_results:
	#	print res, ":", final_results[res] 

if __name__ == '__main__':
	main()
