from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.types 
from pyspark.sql.functions import explode
from pyspark.sql.window import Window




spark = SparkSession.builder \
    .appName("05_pages-meta-current") \
    .master("local") \
    .getOrCreate()


username_ip = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag","contributor") \
    .load("05_pages-meta-current.xml")


revision = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag","revision") \
    .load("05_pages-meta-current.xml")
    

# revision.show(n =5, truncate=True, vertical=False)
# revision.printSchema()

time_stamp = revision.select("timestamp","contributor.username", "contributor.ip")
time_stamp.show()


# username_ip.show(n =20, truncate=True, vertical=False)
# username_ip.printSchema()
# username_ip.groupBy("username", "ip").count().orderBy("username","ip", ascending= False).show(n = 3)
# username_count = username_ip.groupBy("username").agg({"username":"count"}).orderBy("count(username)", ascending = False)

# uc = username_count.withColumnRenamed("count(username)", "Anzahl")
# .show(n = 3, truncate = False, vertical = False)

# ip_count = username_ip.groupBy("ip").agg({"ip":"count"}).orderBy("count(ip)", ascending = False).part
# ic = ip_count.withColumnRenamed("count(ip)", "Anzahl")
# .show(n = 3, truncate = False, vertical = False)

# windowsSpec = Window.partitionBy()


# inner = ic.join(uc, on = ["Anzahl"], how = 'outer')
# inner.orderBy("Anzahl",ascending = False).show(n = 30, truncate=True, vertical=False)
# username_ip.select("revision.contributor.ip").show(n = 30, truncate=True, vertical=False)