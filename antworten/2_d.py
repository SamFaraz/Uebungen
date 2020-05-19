from operator import add
import operator
from pyspark.sql import SQLContext
from pyspark.sql import Window
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
import pyspark

conf = SparkConf().setAppName("part").setMaster("local[*]")
sc = SparkContext(conf = conf)

def Func(lines):
      
    lines = lines.split("|") 
    return  lines[4]

def words(lines):
      
    lines = lines.split(" ") 
    return  lines

text = sc.textFile("part.tbl")
text1 = text.map(Func)
text2 = text1.map(words).collect()
text3 = text1.count()


#for line in text2:
#   if len(line)==3:
#      print(text2)
print("So viele Items haben 3 Woerter in ihrem Namen")
print(text3)



