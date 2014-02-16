#!/usr/local/bin/python2.7

import string, sys

def main():
    count = 0
    
    for line in sys.stdin:
        line = line.strip()
        row  = line.split("\t")

        count += 1
                
        ## take Xth tweet
        if count % 1000 == 0:
            print line

if __name__ == '__main__':
    main()
