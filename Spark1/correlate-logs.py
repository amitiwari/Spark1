from pyspark import SparkContext 
from pyspark import SparkConf
import sys, operator
import re, string, math


def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

def calc_r(n,Sx,Sy,Sx2,Sy2,Sxy):    
    numr= (n*Sxy)-(Sx*Sy)                    
    denomr = math.sqrt((n*Sx2)-(Sx*Sx)) * math.sqrt((n*Sy2)-(Sy*Sy))
    r=numr/denomr
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

rdd2=rdd1.reduceByKey(add_tuples).map(lambda (w,(x,y)):(1,x,y,x*x,y*y,x*y))

rddcalc=rdd2.reduce(add_tuples)

rvalue=calc_r(*rddcalc)

tuples=(rddcalc[0],rddcalc[1],rddcalc[2],rddcalc[3],rddcalc[4],rddcalc[5], rvalue, rvalue*rvalue)

createrdd= sc.parallelize(tuples,1)

createrdd.saveAsTextFile(output)