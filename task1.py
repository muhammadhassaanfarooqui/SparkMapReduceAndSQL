from pyspark import SparkConf, SparkContext
from csv import reader

from operator import add
import sys

def printer(res):
	return ("%s\t%s, %s, %s, %s" % (res[0], res[1][0], res[1][1], res[1][2], res[1][3]))

def parkingMapper(x):
    sNum = x[0]
    precint = x[-16]
    pid = x[-8]
    code = x[2]
    date = x[1]
    return (sNum, (pid, precint, code, date))

def main(sc,filename1, filename2):
    linesParking = sc.textFile(filename1)
    linesOpen = sc.textFile(filename2)
    linesParking = linesParking.mapPartitions(lambda x: reader(x))
    linesOpen = linesOpen.mapPartitions(lambda x: reader(x))
    parkingResult = linesParking.map(lambda x: parkingMapper(x))
    openResult = linesOpen.map(lambda x: (x[0], 1))
    result = parkingResult.subtractByKey(openResult)
    result = result.map(lambda res: printer(res))
    result.saveAsTextFile("task1.out")

if __name__ == "__main__":
    sc = SparkContext()
    filename1 = sys.argv[1]
    filename2 = sys.argv[2]
    main(sc, filename1, filename2)