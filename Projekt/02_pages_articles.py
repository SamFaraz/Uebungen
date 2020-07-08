from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.types
from pyspark.sql import Window
from pyspark.sql.functions import explode
from pyspark import SparkContext, SparkConf


# conf = SparkConf().setAppName("pages-articles").setMaster("local[1]")
# sc = SparkContext(conf = conf)
# sqlContext = SQLContext(sc)


spark = SparkSession.builder \
    .appName("pages-articles") \
    .master("local") \
    .getOrCreate()


df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag","page") \
    .load("pages-articles.xml")
    


# df.show(n = 5 , truncate=True, vertical=False)
df.printSchema()

# df.select("id", "title","revision.timestamp").show(n = 5 , truncate=True, vertical=True)

# a= df.select("id", "title", explode("revision"))
# a.show(n = 1 , truncate=True, vertical=True)

# df.withColumn("neu", explode("revision")).alias("neu01").show(n = 1)