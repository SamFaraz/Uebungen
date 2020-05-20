from operator import add
import operator
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import Window, Row
from pyspark.sql.functions import rand
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
import pyspark
import pyspark.sql.types
from pyspark.sql.types import FloatType,DoubleType,StringType,IntegerType,StructField,StructType,DecimalType

conf = SparkConf().setAppName("supplier").setMaster("local[*]")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


d0 = sc.textFile('supplier.csv')

d1 = d0.map(lambda x: x.split('|'))
d2 = d1.map(lambda x: (int(x[0]), x[1], x[2], int(x[3]), x[4], float(x[5]), x[6] ))

schema = StructType([
         
         StructField('S_SUPPKEY', IntegerType(), True),
         StructField('S_NAME', StringType(), True),
         StructField('S_ADDRESS', StringType(), True),
         StructField('S_NATIONKEY', IntegerType(), True),
         StructField('S_PHONE', StringType(), True),
         StructField('S_ACCTBAL', FloatType(), True),
         StructField('S_COMMENT', StringType(), True)
         ])

df = sqlContext.createDataFrame(d2, schema=schema)


df_sort= df.orderBy('S_ACCTBAL',ascending = True)

df_sort.show(25, False)





