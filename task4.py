from pyspark import SparkConf, SparkContext
from csv import reader

from operator import add
import sys

def printer(res):
	return ("%s\t%d" % (res[0], res[1]))

def mapper(line):
    regState = line[16]
    if ('NY' in regState.upper()):
        return ('NY', 1)
    else:
        return ('Other', 1)

def main(sc,filename):
    lines = sc.textFile(filename)
    lines = lines.mapPartitions(lambda x: reader(x))
    result = lines.map(lambda line: mapper(line)).reduceByKey(lambda x, y: x+y)
    result = result.map(lambda res: printer(res));
    result.saveAsTextFile("task4.out")

if __name__ == "__main__":
    sc = SparkContext()
    filename = sys.argv[1]
    main(sc, filename)