from operator import add
import operator
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import Window, Row 
from pyspark.sql.functions import rand
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
import pyspark
import pyspark.sql.types


conf = SparkConf().setAppName("supplier").setMaster("local[*]")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


d0 = sc.textFile('supplier.csv')

d1 = d0.map(lambda x: x.split('|'))
df = sqlContext.createDataFrame(d1)


df_sort= df.orderBy('_6',ascending=False)

df_sort.show(25, False)








