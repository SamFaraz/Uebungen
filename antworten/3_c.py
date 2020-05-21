from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import Window, Row
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
import pyspark
import pyspark.sql.types
from pyspark.sql.types import FloatType,DoubleType,StringType,IntegerType,StructField,StructType,DecimalType

conf = SparkConf().setAppName("part").setMaster("local[*]")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


d0 = sc.textFile('part.tbl')

d1 = d0.map(lambda x: x.split('|'))
d2 = d1.map(lambda x: (int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6] , float(x[7]), x[8])).collect()



schema = StructType([

         StructField('P_PARTKEY', IntegerType(), True),
         StructField('S_NAME', StringType(), True),
         StructField('P_MFGR', StringType(), True),
         StructField('P_BRAND', StringType(), True),
         StructField('P_TYPE', StringType(), True),
         StructField('P_SIZE', IntegerType(), True),
         StructField('P_CONTAINER', StringType(), True),
         StructField('P_RETAILPRICE', FloatType(), True),
         StructField('S_COMMENT', StringType(), True)
         ])

df = sqlContext.createDataFrame(d2, schema=schema)

Item_Brand = df.groupBy('P_MFGR','P_BRAND').count().orderBy('P_MFGR','P_BRAND')
print("Manufacturer and Brands and Number of Items of each Brand")
#Item_Brand.show(30, False)


Total_Sales = df.groupby('P_BRAND').agg({'P_RETAILPRICE': 'sum'}).orderBy('P_BRAND')

print("")
#Total_Sales.show(30, False)
print("")
df_inner = Item_Brand.join(Total_Sales, on=['P_BRAND'], how='inner').orderBy('P_MFGR','P_BRAND')
print("Manufacturer and Brands and Number of Items of each Brand and Total Sale Price of each Brand:")
print("")
df_inner.show(30, False)



#MFG_Brand = df.dropDuplicates(['P_BRAND']).orderBy('P_MFGR','P_BRAND').select('P_MFGR','P_BRAND')
#
#MFG_Brand.show(50, False)
