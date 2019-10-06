import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()

    parking_v = sc.textFile(sys.argv[1], 1)
    parking_v = parking_v.mapPartitions(lambda x: reader(x))
    
    req_col = parking_v.map(lambda line: (line[2], line[1]))\
        .sortByKey()
    req_col = req_col.groupByKey().map(lambda x : (x[0], list(x[1])))
    
    def days(x):
        week = 0
        end = 0
        for i in range (0, len(x)):
            if x[i] in ['2016-03-05','2016-03-06','2016-03-12','2016-03-13','2016-03-19','2016-03-20','2016-03-26','2016-03-27']:
                end+=1
            else:
                week+=1
        week = float(week/23.00)
        end = float(end/8.00)
        return end, week

    result_v = req_col.map(lambda x: (x[0], days(x[1])))
    result_v = result_v.map(lambda x: "%s\t%.2f, %.2f" %(x[0],x[1][0], x[1][1]))
    result_v.saveAsTextFile("task7.out")

    sc.stop()
