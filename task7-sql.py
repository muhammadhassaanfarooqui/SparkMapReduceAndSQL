from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format
from csv import reader

import sys

ENDDAYS = 8
WEEKDAYS = 23

def getQuery():
    return """select violation_code, day, case day when "Weekend" then total/8 else total/23 end as average
              from
              (select violation_code, day, count(*) as total
              from
              (select violation_code, case 
                                          when dayofmonth(issue_date) in (5, 6, 12, 13, 19, 20, 26, 27) then "Weekend"
                                          else "Weekday" end as day
              from parking)
              group by violation_code, day)"""


def getQuery2():
  return """select case when A.violation_code is not NULL then A.violation_code
            when B.violation_code is not NULL then B.violation_code
            else NULL end as violation_code, 
            case when A.average is NULL then 0.00
            else A.average end as weekend_average, 
            case when B.average is NULL then 0.00
            else B.average end as weekday_average
            from weekendTable A full outer join weekdayTable B on (A.violation_code = B.violation_code)
            where A.violation_code is not NULL or B.violation_code is not NULL"""


def main(spark,filename):
    DF = spark.read.format('csv').options(header='true',inferschema='true').load(filename)
    DF.createOrReplaceTempView("parking")
    query = getQuery()
    tempResult = spark.sql(query)
    weekendDF = tempResult.filter(tempResult.day == "Weekend").createOrReplaceTempView("weekendTable")
    weekdayDF = tempResult.filter(tempResult.day == "Weekday").createOrReplaceTempView("weekdayTable")
    query = getQuery2()
    result = spark.sql(query)
    result.select(format_string('%s\t%.2f, %.2f',result.violation_code, result.weekend_average, result.weekday_average)).write.save("task7-sql.out",format="text")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    filename = sys.argv[1]
    main(spark, filename)