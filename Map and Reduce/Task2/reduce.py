from operator import *
import sys


current_code = None
current_count = 0
new_code = None

for line in sys.stdin:
    line = line.strip()

    new_code, count = line.split('\t', 1)

    try:
        count = int(count)
    except value_consolidateError:
        continue

    if current_code == new_code:
        current_count += count
    else:
        if current_code:
            print '%s\t%s' % (current_code, current_count)
        current_count = count
        current_code = new_code

if current_code == new_code:
    print '%s\t%s' % (current_code, current_count)