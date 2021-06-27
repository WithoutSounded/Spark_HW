import re
import sys
from operator import add
from pyspark.sql import SparkSession


def rCalc(neighbors, rank):
    n = len(neighbors)
    for url in neighbors:
        yield (url, rank/n)


def parseNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PageRank")\
        .getOrCreate()
    
    sc = spark.sparkContext
    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    
    lines = sc.textFile(sys.argv[1])
    
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().partitionBy(4).cache()
    # links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))


    for iteration in range(int(sys.argv[2])):
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: rCalc(url_urls_rank[1][0], url_urls_rank[1][1]))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))

    # ranks.saveAsTextFile('file:///home/pcdm/BigDataSystem/sparkHW/PageRankOutput')
    spark.stop()
