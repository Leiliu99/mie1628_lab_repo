#The following code has parts referenced from:
# https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/

#!/usr/bin/env python3
import sys

#get input
for line in sys.stdin:
    #key: "line"
    #value: "1"
    print('%s\t%s' % ('line', 1))