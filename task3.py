import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    lines_v = sc.textFile(sys.argv[1], 1) 
    lines_v = lines_v.mapPartitions(lambda x: reader(x))

    req_col = lines_v.map(lambda line: (line[2],line[12]))
    initial = req_col.map(lambda x: (x[0], float(x[1])))
    
    sum_with_freq_v = initial.combineByKey(lambda value: (value, 1), lambda x, value: (x[0] + value, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))
    

    average = sum_with_freq_v.map(lambda (label, (value_sum, count)): "%s\t%.2f, %.2f" %(label,value_sum,value_sum/count))
    average.saveAsTextFile("task3.out")

    sc.stop()
