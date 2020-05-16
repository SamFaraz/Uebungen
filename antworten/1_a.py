from pyspark.sql import SQLContext
import pyspark.sql.types
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("readcsv").setMaster("local[1]")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)



df_basket  = sqlContext.read.format('com.databricks.spark.csv').options(header='False').load('filehash.csv')

#Ausgabe der Komplette Tabelle
df_basket.show()

# Erster Versuch,um Duplikate zu verhindern
dup= df_basket.dropDuplicates((['_c1']))

dup.show()

# Anzahl der Duplikate
dupp = df_basket \
    .groupby(['_c1']) \
    .count() \
    .where('count > 1')

dupp.show()


# Inneren Jion und das Ergebnis
inner = dupp.join(df_basket, on = ['_c1'], how = 'inner')
inner.show()

inner.select('_c0').distinct().show()


'''
Diese folgende Zeilen sind nur als Trainieren geschrieben worden
#inner = df_basket.join(dup, on = ['_c1'], how = 'inner')



#dup=df_basket.groupBy("_c1").countDistinct().fr("count &gt;1"))
#dup= df_basket.groupBy(df_basket.columns).agg(f.countDistinct("*")>1)

#dup01 = df_basket.select("_c0","_c1").distinct().show()

#df_basket.intersect(dup).sort("_c0","_c1").show()

'''