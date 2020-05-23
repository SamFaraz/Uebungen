from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *

conf = SparkConf().setAppName("partsupp").setAppName("part").setAppName("supplier").setMaster("local[*]")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


d0 = sc.textFile('partsupp.tbl')

d1 = d0.map(lambda x: x.split('|'))
d2 = d1.map(lambda x: (int(x[0]), int(x[1]), int(x[2]), float(x[3]), x[4])).collect()



schema01 = StructType([

         StructField('PS_PARTKEY', IntegerType(), True),
         StructField('PS_SUPPKEY', IntegerType(), True),
         StructField('PS_AVAILQTY', IntegerType(), True),
         StructField('PS_SUPPLYCOST', FloatType(), True),
         StructField('PS_COMMENT', StringType(), True)
         ])


partsupp = sqlContext.createDataFrame(d2, schema=schema01)



d3 = sc.textFile('part.tbl')

d4 = d3.map(lambda x: x.split('|'))
d5 = d4.map(lambda x: (int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6] , float(x[7]), x[8])).collect()



schema02 = StructType([

         StructField('P_PARTKEY', IntegerType(), True),
         StructField('S_NAME', StringType(), True),
         StructField('P_MFGR', StringType(), True),
         StructField('P_BRAND', StringType(), True),
         StructField('P_TYPE', StringType(), True),
         StructField('P_SIZE', IntegerType(), True),
         StructField('P_CONTAINER', StringType(), True),
         StructField('P_RETAILPRICE', FloatType(), True),
         StructField('P_COMMENT', StringType(), True)
         ])

part = sqlContext.createDataFrame(d5, schema=schema02)


d6 = sc.textFile('supplier.csv')

d7 = d6.map(lambda x: x.split('|'))
d8 = d7.map(lambda x: (int(x[0]), x[1], x[2], int(x[3]), x[4], float(x[5]), x[6] ))

schema03 = StructType([
         
         StructField('S_SUPPKEY', IntegerType(), True),
         StructField('S_NAME', StringType(), True),
         StructField('S_ADDRESS', StringType(), True),
         StructField('S_NATIONKEY', IntegerType(), True),
         StructField('S_PHONE', StringType(), True),
         StructField('S_ACCTBAL', FloatType(), True),
         StructField('S_COMMENT', StringType(), True)
         ])

supplier = sqlContext.createDataFrame(d8, schema=schema03)

part.show()
partsupp.show()
supplier.show()