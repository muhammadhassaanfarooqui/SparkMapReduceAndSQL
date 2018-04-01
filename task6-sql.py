from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format
from csv import reader

import sys


def getQuery():
    return """select * from
              (select plate_id, registration_state, count(*) as total
              from parking
              group by plate_id, registration_state
              order by registration_state, cast(substring(plate_id, 1, 2) as int))
              order by total desc
              limit 20"""



def main(spark,filename):
    DF = spark.read.format('csv').options(header='true',inferschema='true').load(filename)
    DF.createOrReplaceTempView("parking")
    query = getQuery()
    result = spark.sql(query)
    result.select(format_string('%s, %s\t%d',result.plate_id, result.registration_state, result.total)).write.save("task6-sql.out",format="text")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    filename = sys.argv[1]
    main(spark, filename)