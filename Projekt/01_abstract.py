from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.types
from pyspark.sql.functions import explode




spark = SparkSession.builder \
    .appName("01_abstract") \
    .master("local") \
    .getOrCreate()


df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag","doc") \
    .load("01_abstract.xml")
    


# df.show(n = 5 , truncate=True, vertical=False)
# df.printSchema()

url = df.select("url") 
# url.groupby('url').agg({'url': 'count'}).show()
url.show(truncate=False)