import sys
import pyspark
from pyspark.sql.functions import *
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format

spark = pyspark.sql.SparkSession \
    .builder \
    .appName("task7-sql.py") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking_v = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])

parking_v.createOrReplaceTempView("parking_v")

result_v = spark.sql("select p.violation_code, CAST(round(COUNT(IF(date_format(p.issue_date,'EEEE") in ("Saturday","SUnday"),1, NULL))/8,2) as DECIMAL(20,2)) as weekend_average, CAST(round(COUNT(IF(date_format(p.issue_date, "EEEE") not in ("Saturday", "Sunday"), 1, NULL))/23,2) as DECIMAL(20,2)) as weekday_average from parking_v group by p.violation_code')

result_v.select(format_string('%d\t%s, %d' ,result_v.violation_code, result_v.weekend_average, result_v.weekday_average)).write.save("task7-sql.out",format="text")