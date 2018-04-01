from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format
from csv import reader

import sys

# def getQuery():
#     return """select total
#               from
#               ((select count(*) as total
#               from parking
#               where registration_state = 'NY')
#               union
#               (select count(*) as total
#               from parking
#               where registration_state != 'NY'))"""

def getQuery():
    return """select registration_state, sum(total) as total
              from (select case registration_state
                                        when 'NY' then 'NY'
                                        else 'Other' end as registration_state, count(*) as total
              from parking
              group by registration_state)
              group by registration_state"""



def main(spark,filename):
    DF = spark.read.format('csv').options(header='true',inferschema='true').load(filename)
    DF.createOrReplaceTempView("parking")
    query = getQuery()
    result = spark.sql(query)
    result.select(format_string('%s\t%d',result.registration_state, result.total)).write.save("task4-sql.out",format="text")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    filename = sys.argv[1]
    main(spark, filename)