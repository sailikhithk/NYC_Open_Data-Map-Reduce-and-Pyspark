import sys
import string

current_key_consolidate = None
previous_val = None
c = 0

for line in sys.stdin:
    line = line.strip()
    key_consolidate, value_consolidate = line.split('\t',1)
    if key_consolidate == current_key_consolidate:
        c += 1
    else:
        if current_key_consolidate:
            if c == 1:
                print(str(current_key_consolidate) + '\t' + str(previous_val))

        current_key_consolidate = key_consolidate
        c = 1
        previous_val = value_consolidate

if c == 1:
    print(str(key_consolidate) + '\t' + str(value_consolidate))