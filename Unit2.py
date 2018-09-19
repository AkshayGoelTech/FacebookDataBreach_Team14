# -*- coding: utf-8 -*-
#This uses the Lesk algorithm for word sense disambiguation, using the wordnet
#dictionary. The Lesk algorithm needs each word to still be associated with a
#sentence to disambiguate senses, hence the structuring of the lists.

#Fix encoding
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import nltk
from nltk.corpus import wordnet as wn
from nltk.probability import FreqDist
from nltk.wsd import lesk
from nltk.corpus import stopwords
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

#Load Spark Session
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
conf = SparkConf().setAppName('prettify')
sc = SparkContext(conf = conf)
spark = SparkSession.builder.appName('prettify').getOrCreate()
path = "/user/cs4984cs5984f18_team14/rawdata.json"

wordDF = spark.read.json(path)

def penn2morphy(penntag, returnNone=False):
    #default nltk pos_tags are not compatible with wordnet functions
    morphy_tag = {'NN':wn.NOUN, 'JJ':wn.ADJ,
                  'VB':wn.VERB, 'RB':wn.ADV}
    try:
        return morphy_tag[penntag[:2]]
    except:
        return None if returnNone else ''

def get_stopwords():
    #the default stopwords list isn't quite sufficient
	nltk_defaults = stopwords.words('english')
	punctuation = [',', ':', '.', ';', '-', '"', '--', '!', '?', '(', ')', '``', '\'\'']
	custom_words = ['would', 'though', 'it', 'still', 'he', 'at', 'even', 'but', 'like', 'upon', 'a']
	return set(nltk_defaults + punctuation + custom_words)

def mostcommonsyns(text):
    stopWords = get_stopwords()
    sentList = nltk.sent_tokenize(text)
    wordsInSentsPos = [nltk.pos_tag(nltk.word_tokenize(s)) for s in sentList]
    wordsInSentsWnPos = [[(w[0],penn2morphy(w[1])) for w in s if w[0].lower() not in stopWords] for s in wordsInSentsPos]
    #the above returns a list of sentences where each sentence is a list of
    #(word-as-string, pos tag) tuples. Stop words are removed here because pos_tag
    #uses grammatical structure but lesk does not. 
    #This would also be the place to lemmatize, which will help lesk out.
    synsetsList = [lesk(s,w[0],w[1]) for s in wordsInSentsWnPos for w in s]
    return FreqDist([x for x in synsetsList if x is not None])
def freqSum(addends):
    running = FreqDist()
    for x in addends:
        running = running + x
    return running

pysparkSyns = udf(mostcommonsyns,StringType())
synsDF = wordDF.withColumn('synsetcounts',pysparkSyns(wordDF.text))
synsFreqs = freqSum(synsDF.select('synsetcounts').flatMap(lambda x: x).collect())

print(synsFreqs)


#for ss in counts.most_common(30):
#    print(ss[0].lemma_names())
