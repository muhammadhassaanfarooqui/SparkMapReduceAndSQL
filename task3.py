from pyspark import SparkConf, SparkContext
from csv import reader

from operator import add
import sys

def reducer(pair1, pair2):
	return((pair1[0] + pair1[0], pair1[1] + pair1[1]))

def main(sc,filename):
    lines = sc.textFile(filename)
    lines = lines.mapPartitions(lambda x: reader(x))
    result = lines.map(lambda line: (line[2], (1, line[12]))).reduceByKey(lambda pair1, pair2: reducer(pair1, pair2)).collect()
    for res in result:
    	print ("%s\t%s, %s" % (res[0], res[1][0], res[1][1]))


if __name__ == "__main__":
    sc = SparkContext()
    filename = sys.argv[1]
    main(sc, filename)