#!/usr/bin/env python

## adapted from Michael Noll's MapReduce in Python tutorial
## http://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/

from __future__ import division
from operator import itemgetter
import sys

def main():
    if len(sys.argv) < 2:
        print "Usage: avgNReduce.py <number of fields in key>"
        sys.exit(0)

    c_key  = None
    c_Savg = 0
    c_n    = 0
    nkey   = int(sys.argv[1])

    # input comes from STDIN
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()

        # parse the input we got from mapper
        row  = line.split('\t')
        key  = "\t".join( row[0:nkey] )
        Savg = row[nkey]

        # convert count (currently a string) to int
        try:
            Savg = float(Savg)
        except ValueError:
            # count was not a number, so silently
            # ignore/discard this line
            continue

        # this IF-switch only works because Hadoop sorts map output
        # by key (here: word) before it is passed to the reducer
        if c_key == key:
            c_Savg += Savg
            c_n    += 1
        else:
            if c_key:
                # write result to STDOUT
                print '%s\t%s' % (c_key, c_Savg/c_n)
            c_n    = 1
            c_Savg = Savg
            c_key  = key

    # do not forget to output the last word if needed!
    if c_key == key:
        print '%s\t%s' % (c_key, c_Savg/c_n)


if __name__ == '__main__':
    main()
