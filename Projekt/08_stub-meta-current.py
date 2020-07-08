from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.types
from pyspark.sql.functions import explode




spark = SparkSession.builder \
    .appName("08_stub-meta-current") \
    .master("local") \
    .getOrCreate()


df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag","page") \
    .load("08_stub-meta-current.xml")
    


df.show(n = 5 , truncate=True, vertical=False)
df.printSchema()