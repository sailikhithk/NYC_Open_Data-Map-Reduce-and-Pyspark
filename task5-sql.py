import sys
import pyspark
from pyspark.sql.functions import *
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format

spark = pyspark.sql.SparkSession \
    .builder \
    .appName("task5-sql.py") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking_v = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])

parking_v.createOrReplaceTempView("parking_v")

result_v = spark.sql("select plate_id, registration_state, count(*) as counter from parking_v group by plate_id, registration_state order by counter desc limit 1")

result_v.select(format_string('%d\t%s, %d' ,result_v.plate_id, result_v.registration_state, result_v.counter)).write.save("task5-sql.out",format="text")