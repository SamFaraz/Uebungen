from pyspark.sql import SQLContext
import pyspark.sql.types
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("stub_articles").setMaster("local[1]")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)



df_basket  = sqlContext.read.format('com.databricks.spark.csv').options(header='False').load('stub_articles.csv')
df_basket.show()