#The following code has parts referenced from:
# https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/

#!/usr/bin/env python3
import sys

lines_count = 0
for line in sys.stdin:
    # parse the input we got from mapper.py
    _, count = line.split('\t', 1)
    lines_count += int(count)

#key:"line", value:"<total number of lines>"
print('%s\t%s' % ('line', lines_count))