from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.types
from pyspark.sql import Window



spark = SparkSession.builder \
    .appName("02_pages-articles") \
    .master("local") \
    .getOrCreate()


df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag","page") \
    .load("02_pages-articles.xml")
    


# df.show(n = 5 , truncate=True, vertical=False)
df.printSchema()

# df.select("id", "title","revision.timestamp").show(n = 5 , truncate=True, vertical=True)
