import sys
import pyspark
from pyspark.sql.functions import *
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format

spark = pyspark.sql.SparkSession \
    .builder \
    .appName("task1-sql.py") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking_v = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
open_v = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])


parking_v.createOrReplaceTempView("parking_v")
open_v.createOrReplaceTempView("open_v")

result_v = spark.sql("SELECT parking_v.summons_number, plate_id, violation_precinct, violation_code, parking_v.issue_date FROM parking_v LEFT JOIN open_v ON parking_v.summons_number=open_v.summons_number WHERE open_v.summons_number is null ORDER BY parking_v.summons_number ASC ")

result_v.select(format_string('%d\t%s, %d, %d, %s' ,result_v.summons_number,result_v.plate_id,result_v.violation_precinct,result_v.violation_code,date_format(result_v.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")