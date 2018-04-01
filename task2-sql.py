from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format
from csv import reader

import sys

def getQuery():
    return """select violation_code, count(*) as num_violations
              from parking
              group by violation_code"""

def main(spark,filename):
    DF = spark.read.format('csv').options(header='true',inferschema='true').load(filename)
    DF.createOrReplaceTempView("parking")
    query = getQuery()
    result = spark.sql(query)
    result.select(format_string('%d\t%d',result.violation_code,result.num_violations)).write.save("task2-sql.out",format="text")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    filename = sys.argv[1]
    main(spark, filename)