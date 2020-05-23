from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *

conf = SparkConf().setAppName("partsupp").setMaster("local[*]")
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

different = partsupp.groupby('PS_SUPPKEY').agg({'PS_PARTKEY': 'count'}).orderBy('PS_SUPPKEY')
different.show(1000, False)