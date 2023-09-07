from pyspark.sql import SparkSession
spark = SparkSession.\
        builder.\
        appName("contadordepalavras").\
        master("local[*]").\
        getOrCreate()
sc = spark.sparkContext
file = sc.textFile('/var/README.md')
print('Número de linhas no arquivo: %s' % file.count())
import re
words = file.flatMap(lambda line: re.split('\W+', line.lower().strip()))
words = words.filter(lambda x: len(x) > 3)
words = words.map(lambda w: (w,1))
from operator import add
words = words.reduceByKey(add)
wordsFreq = words.map(lambda x:(x[0],x[1])).sortByKey(False)
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
schema = StructType([
  StructField("Palavras", StringType(), True), \
  StructField("Frequencia", StringType(), False), \
 ])
df1 = spark.createDataFrame(data=wordsFreq, schema=schema)
contador = wordsFreq.count()
print("Quantidade de palavras: ", contador)
df1.sort(['Frequencia'],ascending=False).show(218)
spark.sparkContext.stop()
