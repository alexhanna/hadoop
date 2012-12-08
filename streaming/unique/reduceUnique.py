#!/usr/bin/env python

import sys

def main():

    currentKey = None

    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()

        (key, json) = line.split("\t")

        ## if the keys are the same, then just skip this 
        if currentKey == key:
        	continue
        else:
        	## else, print the tweet
        	print json
        	currentKey = key

if __name__ == '__main__':
    main()