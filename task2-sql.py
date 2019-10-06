import sys
import pyspark
from pyspark.sql.functions import *
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format

spark = pyspark.sql.SparkSession \
    .builder \
    .appName("task2-sql.py") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking_v = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])



parking_v.createOrReplaceTempView("parking_v")

result_v = spark.sql("select violation_code, count(*) as counter from parking group by violation_code ORDER BY parking_v.summons_number ASC ")

result_v.select(format_string('%d\t%s, %d, %d, %s' ,result_v.violation_code,result_v.count(*))).write.save("task2-sql.out",format="text")