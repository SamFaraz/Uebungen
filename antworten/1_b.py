
import operator
from operator import add
from pyspark.sql import SQLContext
import pyspark.sql.types
from pyspark.sql import Window
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
import pyspark

conf = SparkConf().setAppName("1_b").setMaster("local[1]")
sc = SparkContext(conf = conf)


def Func(lines):
      lines = lines.lower()
      lines = lines.replace('.','').replace(':', '').replace(';','').replace('-','').replace('!','').replace('?','').replace('(','').replace(')','').replace(',','')
      lines = lines.split() 
      return lines



text = sc.textFile("informatic.txt")

text1 = text.flatMap(Func)

stopwords = ['is','am','are','the','for','a','of','and','in', 'to', 'from', 'that', 'this', 'was']
text2 = text1.filter(lambda x: x not in stopwords)

text2 = text2.map(lambda x:(x, 1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)



original_text = text2.collect()


#wordcount = text1.reduceByKey(operator.add)
#sorted_wordcount = wordcount.sortBy(lambda x: x[1, False])
#for word,count in sorted_wordcount.toLocalIterator():
#    print(u"{} : {}".format(word, count))

for line in original_text:
    print(line[0],"kommt",line[1], " mal vor")

x = len(original_text)

print("Gesamtzahl der vorkommenden unterschiedlichen Worte ist  ", x)
#wordcount  = sqlContext.read.format('com.databricks.spark.txt').options(header='False').load('informatic.txt')

#wordcount.show()

#nltk package für StopWords 