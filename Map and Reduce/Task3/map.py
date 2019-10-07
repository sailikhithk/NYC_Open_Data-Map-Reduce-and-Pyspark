import sys
import string

for line in sys.stdin:
    line = line.strip().replace("\"","")
    row = line.split(',')
    key_consolidate = row[2] 

    if len(key_consolidate)>3 or len(key_consolidate)<3:
        continue
    if row[12]:
        value_consolidate = float(row[12]),1.00 
    else:
        value_consolidate = 0.00,1.00
    key_value_consolidate = {key_consolidate:value_consolidate}
    sys.stdout.write(str(key_value_consolidate).replace("{","").replace("}","").replace(":","\t").replace("(","").replace(")","").replace("'","")+"\n")