from operator import add
import operator
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import Window, Row
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
import pyspark
import pyspark.sql.types
from pyspark.sql.types import StringType, StructType, StructField

conf = SparkConf().setAppName("readcsv").setMaster("local[1]")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)



 
df= sqlContext.read.format('csv').options(header='false', inferSchema='false').load('supplier.tbl')
df.show(truncate = False)
#textrdd = text.map(lambda x: x.split("|"))
#df = sqlContext.createDataFrame(textrdd,('1', '2', '3', '4', '5'))
#df.show()





