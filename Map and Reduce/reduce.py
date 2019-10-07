from __future__ import division
from operator import itemgetter
import sys


current_license = None
current_total = 0
new_license = None
count = 1
current_val = None

for line in sys.stdin:
    line = line.strip().replace(",","").split()
    new_license = line[0] 
    total = float(line[1]) 
    average = float(line[2]) 

    if current_license == new_license:
        current_total += total
        count = count+1
    else:
        average = float(current_total/count)
        if current_license:
            print (str(current_license)+"\t"+ "%.2f" %current_total +","+"%.2f" %average)
            #dummy = 1
        current_total = total
        current_license = new_license
        average = total
        count = 1


if current_license == new_license:
    average = float(current_total/count)
    print (str(current_license)+"\t"+ current_total +","+ average)