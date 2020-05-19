from operator import add
import operator
from pyspark.sql import SQLContext
from pyspark.sql import Window
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
import pyspark

conf = SparkConf().setAppName("supplier").setMaster("local[*]")
sc = SparkContext(conf = conf)

def Func(lines):
      
    lines = lines.split("|") 
    return  lines[1], lines[3]

text = sc.textFile("supplier.tbl")
text1 = text.map(Func).collect()

for line in text1:
    print(line)


