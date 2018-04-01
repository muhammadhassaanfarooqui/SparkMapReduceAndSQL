from pyspark import SparkConf, SparkContext
from csv import reader

from operator import add
import sys

WEEKEND_DAYS = [5, 6, 12, 13, 19, 20, 26, 27]
NUM_WEEKDAYS = 23.0
NUM_WEEKENDDAYS = 8.0

def printer(res):
    return ("%s\t%.2f, %.2f" % (res[0], res[1][0], res[1][1]))

def flatmapper(line):
    _, _, day = line[1].split('-')
    vCode = int(line[2])
    if(int(day) in WEEKEND_DAYS):
        return [((vCode, "Weekend_Day"), 1.0), ((vCode, "Weekday"), 0.0)]
    elif(int(day) not in WEEKEND_DAYS):
        return [((vCode, "Weekday"), 1.0), ((vCode, "Weekend_Day"), 0.0)]
    else:
        return [((vCode, "Weekday"), 0.0), ((vCode, "Weekend_Day"), 0.0)]


def secondMapper(x):
    return (x[0][0], (x[0][1], x[1]))

def secondReducer(x, y):
    weekendOrDay = x[0]
    if(weekendOrDay == "Weekend_Day"):
        return (x[1]/NUM_WEEKENDDAYS, y[1]/NUM_WEEKDAYS)
    else:
        return (y[1]/NUM_WEEKENDDAYS, x[1]/NUM_WEEKDAYS)

def main(sc,filename):
    lines = sc.textFile(filename)
    lines = lines.mapPartitions(lambda x: reader(x))
    result = lines.flatMap(lambda line: flatmapper(line)).reduceByKey(lambda x, y: x+y)
    result = result.map(lambda x: secondMapper(x)).reduceByKey(lambda x, y: secondReducer(x, y))
    result = result.map(lambda res: printer(res))
    result.saveAsTextFile("task7.out")

if __name__ == "__main__":
    sc = SparkContext()
    filename = sys.argv[1]
    main(sc, filename)