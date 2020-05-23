from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("partsupp").setMaster("local[*]")
sc = SparkContext(conf = conf)

def Func(lines):
      
    lines = lines.split("|") 
    return  lines[1], lines[0]

text = sc.textFile("partsupp.tbl")
text1 = text.map(Func).sortBy(lambda x:x[0], ascending=True).countByKey()



print(text1)

