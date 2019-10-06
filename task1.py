import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    

    parking_v = sc.textFile(sys.argv[1], 1)
    parking_v = parking_v.mapPartitions(lambda x: reader(x))
    parking_v = parking_v.filter(lambda line: len(line)>1) \
        .map(lambda line: (line[0], str(line[14]) + ', ' + str(line[6]) + ', ' + str(line[2]) + ', ' + str(line[1])))

    open_v = sc.textFile(sys.argv[2],1)
    open_v = open_v.mapPartitions(lambda x: reader(x)) 
    open_v = open_v.filter(lambda line: len(line)>1) \
        .map(lambda line: (line[0], str(line[1]) + ', ' + str(line[5]) + ', ' + str(line[7]) + ', ' + str(line[9])))

    join_v=parking_v.join_v(open_v)

    get_result_v=parking_v.subtractByKey(join_v)
    
    output_v = get_result_v.map(lambda r:"\t".join_v([str(c) for c in r]))
    
    output_v.saveAsTextFile("task1.out")
    
    sc.stop()
