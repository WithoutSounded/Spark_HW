from pyspark.sql import SparkSession
import os
from operator import add

def f(x):
    return (os.path.basename(x[0]), x[1].replace('\n', ' ').strip())

def f_2(x):
    file= x[0]
    txt= x[1]
    txt = x[1].lower().split()
    return (x[0], txt)

def f_3(x):
    file = x[0]
    words = x[1]
    o_l = []
    for i in words:
        o_l.append((i, file))
    return o_l

def f_4(x):
    return (x[0], [x[1]])


spark = SparkSession\
    .builder\
    .appName("Inverted_Index")\
    .getOrCreate()
sc = spark.sparkContext

rdd = sc.wholeTextFiles('file:///home/pcdm/BigDataSystem/sparkHW/invertedIndex_data/*')
data = rdd.collect()
output_readFile=[]
for data_f in data:
    output_readFile.append(f(data_f))

data_rdd = sc.parallelize(output_readFile)\
.map(f_2)\
.flatMap(f_3).distinct()\
.map(f_4).reduceByKey(add)

for line in data_rdd.collect():
    print(line)

