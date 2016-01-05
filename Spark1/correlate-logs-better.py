from pyspark import SparkContext 
from pyspark import SparkConf
import sys, operator
import re, string, math

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

def calc_r(num1, denom1,denom2):    
    denomr=math.sqrt(denom1) * math.sqrt(denom2)
    r=num1/denomr
    return r

linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")

def find_match(line):
    matcher= linere.match(line)    
    if(matcher):
        hostname=matcher.group(1)
        numBytes=int(matcher.group(4))               
        return (hostname,(1,numBytes))
    

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('correlate logs')
sc = SparkContext()

text = sc.textFile(inputs)

rdd1=text.map(lambda line: find_match(line)).filter(lambda x: x is not None)

rdd2=rdd1.reduceByKey(add_tuples).cache()

rddavg1=rdd2.map(lambda (w,(x,y)): (1,x,y))

rddavg2=rddavg1.reduce(add_tuples)

xmean= rddavg2[1]/rddavg2[0]
ymean= rddavg2[2]/rddavg2[0]

rddavgfinally= rdd2.map(lambda (w,(x,y)):(((x-xmean)*(y-ymean)),((x-xmean)*(x-xmean)),((y-ymean)*(y-ymean))))

rddcalc=rddavgfinally.reduce(add_tuples)

rvalue=calc_r(*rddcalc)

tuples=(rvalue, rvalue*rvalue)

createrdd= sc.parallelize(tuples,1)

createrdd.saveAsTextFile(output)