from pyspark.sql.dataframe import DataFrame
import operator
from operator import add
from pyspark.sql import SQLContext
from pyspark.sql.types import FloatType
from pyspark.sql import Window
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
import pyspark

conf = SparkConf().setAppName("supplier").setMaster("local[*]")
sc = SparkContext(conf = conf)

def Func(lines):
      
      lines = lines.split("|") 
      return lines[5]

text = sc.textFile("supplier.tbl")
text1 = text.map(Func)

#zipped = text1.zipWithIndex()
#positive = zipped.filter(lambda x:float(x)>0).keys()

text2 = text1.filter(lambda x:float(x)>0).count()
print(text2)

#original_text = text2.collect()
#print(original_text)

#for line in original_text:
#    print(line)

