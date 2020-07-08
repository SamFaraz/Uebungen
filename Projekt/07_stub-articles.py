from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.types
from pyspark.sql.functions import explode




spark = SparkSession.builder \
    .appName("07_stub-articles") \
    .master("local") \
    .getOrCreate()


df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag","revision") \
    .load("07_stub-articles.xml")
    


df.show(n = 5 , truncate=True, vertical=False)
df.printSchema()