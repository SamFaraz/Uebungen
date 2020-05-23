
from pyspark.sql.functions import col, size, udf
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *

conf = SparkConf().setAppName("part").setMaster("local[*]")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


d0 = sc.textFile('part.tbl')

d1 = d0.map(lambda x: x.split('|'))
d2 = d1.map(lambda x: (int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6] , float(x[7]), x[8])).collect()



schema = StructType([

         StructField('P_PARTKEY', IntegerType(), True),
         StructField('P_NAME', StringType(), True),
         StructField('P_MFGR', StringType(), True),
         StructField('P_BRAND', StringType(), True),
         StructField('P_TYPE', StringType(), True),
         StructField('P_SIZE', IntegerType(), True),
         StructField('P_CONTAINER', StringType(), True),
         StructField('P_RETAILPRICE', FloatType(), True),
         StructField('P_COMMENT', StringType(), True)
         ])

size_ = udf(lambda xs: len(xs), IntegerType())

df = sqlContext.createDataFrame(d2, schema=schema)
df = df.select('P_NAME')
df.where(size_(col('P_NAME')) == 3).describe().show()