from pyspark import SparkConf, SparkContext
from csv import reader

from operator import add
import sys

def printer(res):
	return ("%s, %s\t%d" % (res[0][0], res[0][1], res[1]))

def mapper(line):
    pid = line[14]
    regState = line[16]
    return ((pid, regState), 1)

def main(sc,filename):
    lines = sc.textFile(filename)
    lines = lines.mapPartitions(lambda x: reader(x))
    result = lines.map(lambda line: mapper(line)).reduceByKey(lambda x, y: x+y)
    result = result.sortByKey().sortBy(lambda a: a[1], False).take(20)
    result = sc.parallelize(result)
    result = result.map(lambda res: printer(res))
    result.saveAsTextFile("task6.out")

if __name__ == "__main__":
    sc = SparkContext()
    filename = sys.argv[1]
    main(sc, filename)