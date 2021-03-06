from pyspark import SparkConf, SparkContext
from csv import reader

from operator import add
import sys

def printer(res):
	return ("%s\t%.2f, %.2f" % (res[0], res[1][1], res[1][1]/res[1][0]))

def reducer(pair1, pair2):
	return((pair1[0] + pair2[0], pair1[1] + pair2[1]))

def main(sc,filename):
    lines = sc.textFile(filename)
    lines = lines.mapPartitions(lambda x: reader(x))
    result = lines.map(lambda line: (line[2], (1, float(line[12])))).reduceByKey(lambda pair1, pair2: reducer(pair1, pair2))
    result = result.map(lambda res: printer(res));
    result.saveAsTextFile("task3.out")

if __name__ == "__main__":
    sc = SparkContext()
    filename = sys.argv[1]
    main(sc, filename)