import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    parking_v = sc.textFile(sys.argv[1], 1)
    parking_v = parking_v.mapPartitions(lambda x: reader(x))

    req_col = parking_v.map(lambda line: (line[2]))

    freq_violation_types = req_col.map(lambda x: (x,1)).reduceByKey(add)

    result_v = freq_violation_types.map(lambda x: x[0] + '\t' + str(x[1]))
    
    result_v.saveAsTextFile("task2.out")

    sc.stop()
