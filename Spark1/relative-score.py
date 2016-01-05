from pyspark import SparkContext 
from pyspark import SparkConf
import sys, operator
import re, string
import json

def add_pairs((a,b), (c,d)):
    return ((a+c), (b+d))

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('relative-score')
sc = SparkContext()

text = sc.textFile(inputs)

jsonrdd=text.map(lambda x: json.loads(x)).cache()

jsonrddavg=jsonrdd.map(lambda line: (line['subreddit'],(line['score'],1))) \
    .reduceByKey(add_pairs).coalesce(1).map(lambda (w,(a,b)):(w,float(a)/b)).filter(lambda (w,x): x>0)
    
    
jsonrdd1=jsonrdd.map(lambda line: (line['subreddit'],line))

jsonjoin=jsonrddavg.join(jsonrdd1)

jsonrdd2=jsonjoin.map(lambda (w,(subredditavg,comments)) : ((comments['author']), (comments['score']/subredditavg)))

jsonpopular=jsonrdd2.sortBy(lambda (w,c): (-c,w)).map(lambda (w,c): u"%s %.15f" % (w, c))

jsonpopular.saveAsTextFile(output)
