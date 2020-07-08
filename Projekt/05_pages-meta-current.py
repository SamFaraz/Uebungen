from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.types
from pyspark.sql.functions import explode




spark = SparkSession.builder \
    .appName("05_pages-meta-current") \
    .master("local") \
    .getOrCreate()


df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag","contributor") \
    .load("05_pages-meta-current.xml")
    


# df.show(n =20, truncate=True, vertical=False)
# df.printSchema()

df.groupBy("username").count().show(n = 3, truncate = False, vertical = True)

# df.select("revision.contributor.ip").show(n = 30, truncate=True, vertical=False)