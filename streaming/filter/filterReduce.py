#!/usr/bin/env python
# from http://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/

from operator import itemgetter
import sys

c_key   = None
c_count = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    date, word, count = line.split('\t')
    key = date + "\t" + word

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if c_key == key:
        c_count += count
    else:
        if c_key:
            # write result to STDOUT
            print '%s\t%s' % (c_key, c_count)
        c_count = count
        c_key   = key

# do not forget to output the last word if needed!
if c_key == key:
    print '%s\t%s' % (c_key, c_count)
