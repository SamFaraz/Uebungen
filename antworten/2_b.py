from pyspark.sql import Window, SQLContext
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


text2 = text1.filter(lambda x:float(x)>0).count()
print("The Number of Supplier with positive account balance:")
print(text2)
