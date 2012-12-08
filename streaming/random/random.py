#!/usr/bin/env python

import json, string, sys

def main():
    count = 0
    
    for line in sys.stdin:
        line = line.strip()
        row  = line.split("\t")

        count += 1
                
        ## take every tenth tweet
        if count % 10 == 0:
            print line

if __name__ == '__main__':
    main()
