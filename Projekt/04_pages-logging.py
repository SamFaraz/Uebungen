from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.types
from pyspark.sql.functions import explode




spark = SparkSession.builder \
    .appName("04_pages-logging") \
    .master("local") \
    .getOrCreate()


df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag","logitem") \
    .load("04_pages-logging.xml")
    


df.show(n = 5 , truncate=True, vertical=False)
df.printSchema()

logtitle = df.groupby('logtitle').agg({'logtitle': 'count'})
logtitle.show()