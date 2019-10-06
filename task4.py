import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    parking_v = sc.textFile(sys.argv[1], 1)
    req_col = parking_v.mapPartitions(lambda x: reader(x))
    req_col = req_col.map(lambda x: x[16])
    
    def state(x):
        if x == 'NY': 
            return ('NY', 1)
        else:
            return ('Other', 1)

   

    freq = req_col.map(lambda x : state(x))

    result_v = freq.reduceByKey(add)
    result_v = result_v.map(lambda x: x[0] + '\t' + str(x[1]))
    result_v.saveAsTextFile("task4.out")

    sc.stop()
