#!/usr/bin/env python

import json, os, re, string, sys, time

latinList   = []
#unicodeList = []

def loadKeywords():
    f = open('latinKeywords.txt', 'r')
    for line in f:
        line = line.lower()
        latinList.append( line.strip() )
    f.close()

    # if os.path.exists('unicodeKeywords.txt'):
    #     f = open('unicodeKeywords.txt', 'r')
    #     for line in f:
    #         unicodeList.append( line.strip() )
    #     f.close()

def main():
    loadKeywords()    

    for line in sys.stdin:
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

            date = time.strftime('%Y-%m-%d %H:00:00', dt)

            ## turn text into lower case
            text = data['text'].lower()
            rt   = ''
            if 'retweeted_status' in data:
                rt = data['retweeted_status']['text'].lower()

            for word in latinList:
                if rt != '':
                    if word in rt:
                        print '%s\t%s\t%s' % (date, word, 1)
                elif word in text:
                    print '%s\t%s\t%s' % (date, word, 1)                    

if __name__ == '__main__':
    main()
    
