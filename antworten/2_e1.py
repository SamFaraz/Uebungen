from operator import add
import operator
from pyspark.sql import SQLContext
from pyspark.sql import Window
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
import pyspark

conf = SparkConf().setAppName("partsupp").setMaster("local[*]")
sc = SparkContext(conf = conf)

def Func(lines):
      
    lines = lines.split("|") 
    return  lines[1], lines[0]

text = sc.textFile("partsupp.tbl")
text1 = text.map(Func).sortBy(lambda x:x[0], ascending=True).countByKey()

#for line in text1:
    #print(line)

print(text1)

