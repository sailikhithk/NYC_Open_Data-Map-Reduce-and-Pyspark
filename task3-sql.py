import sys
import pyspark
from pyspark.sql.functions import *
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format

spark = pyspark.sql.SparkSession \
    .builder \
    .appName("task3-sql.py") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

open_v = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])

open_v.createOrReplaceTempView("open_v")

result_v = spark.sql("select open_v.license_type, sum(open_v.amount_due) as sums, avg(open_v.amount_due) as avgs from open_v group by license_type")

result_v.select(format_string('%d\t%s, %d, %d, %s' ,result_v.license_type,result_v.sums)).write.save("task3-sql.out",format="text")