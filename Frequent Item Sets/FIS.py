from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth

def splitf(x):
    x = x.split(' ', 1)
    x[1] = x[1].replace(' ','').split(',')
    return x[0], x[1]
    

spark = SparkSession\
    .builder\
    .appName("Freq_items_sets")\
    .getOrCreate()
sc = spark.sparkContext

data_path = 'file:///home/pcdm/BigDataSystem/sparkHW/FIS.data'
textFile = sc.textFile(data_path)

threshold = 4


df = textFile.map(splitf).toDF(['tid','items'])
support = threshold/df.count()

fpGrowth = FPGrowth(itemsCol="items", minSupport=support, minConfidence=0.6)
model = fpGrowth.fit(df)


model.freqItemsets.show()

