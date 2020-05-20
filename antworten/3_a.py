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

schema = StructType([
        
         StructField('S_SUPPKEY', StringType(), True),
         StructField('S_NAME', StringType(), True),
         StructField('S_ADDRESS', StringType(), True),
         StructField('S_NATIONKEY', StringType(), True),
         StructField('S_PHONE', StringType(), True),
         StructField('S_ACCTBAL', StringType(), True),
         StructField('S_COMMENT', StringType(), True),
         StructField('Null', StringType(), True)
         ])

df = sqlContext.createDataFrame(d1, schema=schema)
#df = sqlContext.createDataFrame(d1)

df_sort= df.orderBy('S_ACCTBAL',ascending = True)

df_sort.show()



#df.withColumn("_6", df["account"].cast(FloatType())).drop("account").withColumnRenamed("_6", "account")




