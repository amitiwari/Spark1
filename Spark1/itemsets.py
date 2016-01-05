from pyspark import SparkContext 
from pyspark import SparkConf
from pyspark.mllib.fpm import FPGrowth
import sys, operator
import re, string



inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('frequent itemsets')
sc = SparkContext()

text = sc.textFile(inputs)

transactions = text.map(lambda line: map(int,line.split()))

model = FPGrowth.train(transactions, 0.0002).freqItemsets().map(lambda (w,z):(sorted(w),z))

modelsort=model.sortBy(lambda (w,c): (-c,w)).map(lambda (w,c): u"%s %i" % (w, c)).take(10000)

modelsort1=sc.parallelize(modelsort,1)

modelsort1.saveAsTextFile(output)