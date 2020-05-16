import operator
from operator import add
from pyspark.sql import SQLContext
from pyspark.sql import Window
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
import pyspark

conf = SparkConf().setAppName("part").setMaster("local[*]")
sc = SparkContext(conf = conf)

def Func(lines):
      
    lines = lines.split("|") 
    return lines[2],lines[3]

text = sc.textFile("part.tbl")
text1 = text.map(Func)

text2 = text1.filter(lambda x: x[0] == "Manufacturer#1")
text3 = text1.filter(lambda x: x[0] == "Manufacturer#2")
text4 = text1.filter(lambda x: x[0] == "Manufacturer#3")
text5 = text1.filter(lambda x: x[0] == "Manufacturer#4")
text6 = text1.filter(lambda x: x[0] == "Manufacturer#5")

original_text1 = text2.countByValue()
#original_text2 = text3.distinct().collect()
#original_text3 = text4.distinct().collect()
#original_text4 = text5.distinct().collect()
#original_text5 = text6.distinct().collect()
print(original_text1)

#for line in original_text1:
#    print(line)
#for line in original_text2:
#    print(line)
#for line in original_text3:
#    print(line)
#for line in original_text4:
#    print(line)
#for line in original_text5:
#    print(line)

