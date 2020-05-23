from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("part").setMaster("local[*]")
sc = SparkContext(conf = conf)

def Func(lines):
      
    lines = lines.split("|") 
    return  lines[1]

def words(lines):
      
    lines = lines.split(" ") 
    return  lines


text = sc.textFile("part.tbl")
text1 = text.map(Func)
text2 = text1.map(words).filter(lambda x: x == 3).count()



print("So viele Items haben 3 Woerter in ihrem Namen")
print(text2)

