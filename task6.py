import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    parking_v = sc.textFile(sys.argv[1], 1)
    req_col = parking_v.mapPartitions(lambda x: reader(x))
    

    numberplatecount = req_col.map(lambda x: ((x[14],x[16]),1)) \
        .reduceByKey(add).takeOrdered(20, key  = lambda x: -x[1])
    numberplatecount = sc.parallelize(numberplatecount)
    
    result_v_v = numberplatecount.map(lambda x: str(x[0]).replace("'","").replace('(','').replace(')','') + '\t' + str(x[1]))
    result_v_v.saveAsTextFile("task6.out")

    sc.stop()
