import sys
import string

import csv

for line in sys.stdin:
    line = line.strip()
    row = line.split(',')
    key_consolidate = row[2]
    value_consolidate = 1
    key_consolidate_value = {key_consolidate:value_consolidate}
    sys.stdout.write(str(key_consolidate_value).replace("{","").replace("}","").replace(":","\t").replace("(","").replace(")","").replace("'","") + "\n")
