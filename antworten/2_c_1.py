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
    return  lines[2],lines[3]

def Funcc(lines):
      
    lines = lines.split("|") 
    return  lines[3],lines[7]



text = sc.textFile("part.tbl")
text1 = text.map(Func)
text2 = text.map(Funcc)

sort1 = text1.distinct().sortBy(lambda x:x[0], ascending=True).sortBy(lambda y:y[1], ascending = True)
sort2 = text2.sortBy(lambda x:x[0], ascending=True)

original_text = sort1.collect()
count_by_key = sort2.countByKey()



print("Manufacturer and Brands:")
for line in original_text:
    print(line)

print("Number of Items of each Brand")
print(count_by_key)

