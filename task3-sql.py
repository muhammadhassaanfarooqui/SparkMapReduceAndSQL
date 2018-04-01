from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format
from csv import reader

import sys

def getQuery():
    return """select license_type, sum(amount_due) as Total, avg(amount_due) as Average
              from open
              group by license_type"""

def main(spark,filename):
    DF = spark.read.format('csv').options(header='true',inferschema='true').load(filename)
    DF.createOrReplaceTempView("open")
    query = getQuery()
    result = spark.sql(query)
    result.select(format_string('%s\t%.2f, %.2f',result.license_type,result.Total,result.Average)).write.save("task3-sql.out",format="text")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    filename = sys.argv[1]
    main(spark, filename)