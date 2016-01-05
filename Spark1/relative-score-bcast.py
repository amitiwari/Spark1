from pyspark import SparkContext 
from pyspark import SparkConf
import sys, operator
import re, string
import json

def add_pairs((a,b), (c,d)):
    return ((a+c), (b+d))

def cal_relscore(broadcasted, comments):
    dictaverages =  broadcasted.value[comments['subreddit']]
    relative_score= float(comments['score'])/dictaverages    
    return relative_score

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('relative-score')
sc = SparkContext()

text = sc.textFile(inputs)

jsonrdd=text.map(lambda x: json.loads(x)).cache()

jsonrddavg=jsonrdd.map(lambda line: (line['subreddit'],(line['score'],1))) \
    .reduceByKey(add_pairs).map(lambda (w,(a,b)):(w,float(a)/b)).filter(lambda (w,x): x>0)    

broadcasted = sc.broadcast(dict(jsonrddavg.collect())) 
    
jsonrdd1=jsonrdd.map(lambda line: (line['subreddit'],line))

jsonrelative=jsonrdd1.map(lambda (subredditname,comments): ((comments['author']),(cal_relscore(broadcasted,comments))))

jsonpopular=jsonrelative.sortBy(lambda (w,c): (-c,w)).map(lambda (w,c): u"%s %.15f" % (w, c))

jsonpopular.saveAsTextFile(output)
