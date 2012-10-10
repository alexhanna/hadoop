#!/usr/bin/env python

import json, os, re, string, sys, time

def main():
    for line in sys.stdin:
        if len(line) == 0:
            break

        v = line.strip()

        data = ''
        try:
            data = json.loads(v)
        except ValueError as detail:
            sys.stderr.write(v + "\n")
            sys.stderr.write(detail.__str__() + "\n")
            continue
                
        if 'text' in data:
            # Parse data in the format of
            # Sat Mar 12 01:49:55 +0000 2011
            d  = string.split( data['created_at'], ' ')
            ds = ' '.join([d[1], d[2], d[3], d[5] ])
            dt = time.strptime(ds, '%b %d %H:%M:%S %Y')

            date = time.strftime('%Y-%m-%d %H:%M:00', dt)

            ## turn text into lower case
            print '%s\t%s' % (date, 1)

if __name__ == '__main__':
    main()
    
