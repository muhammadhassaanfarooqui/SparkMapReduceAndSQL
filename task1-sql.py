from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format
from csv import reader

from operator import add
import sys

def getQuery():
    return "select summons_number, plate_id, violation_precinct, violation_code, issue_date from parking where summons_number in (select * from diffVals)";

def main(spark,filename1, filename2):
    parkingDF = spark.read.format('csv').options(header='true',inferschema='true').load(filename1)
    openDF = spark.read.format('csv').options(header='true',inferschema='true').load(filename2)
    parkingDF.createOrReplaceTempView("parking")
    openDF.createOrReplaceTempView("open")
    parkingDF.select('summons_number').subtract(openDF.select('summons_number')).createOrReplaceTempView("diffVals")
    query = getQuery()
    result = spark.sql(query)
    result.select(format_string('%d\t%s, %d, %d, %s',result.summons_number,result.plate_id,result.violation_precinct,result.violation_code,date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    filename1 = sys.argv[1]
    filename2 = sys.argv[2]
    main(spark, filename1, filename2)