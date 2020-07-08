from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.types
from pyspark.sql.functions import explode




spark = SparkSession.builder \
    .appName("03_pages-articles-multistream") \
    .master("local") \
    .getOrCreate()


df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag","page") \
    .load("03_pages-articles-multistream.xml")
    


df.show(n = 5 , truncate=True, vertical=False)
df.printSchema()