#!/usr/bin/env python

import json, os, re, string, sys, time

#latinList   = []
#unicodeList = []

wordList = []

## arabic punctuation -- replace with space
arabicPunctuation = u'\u060C\u061B\u061F\u066B\u066C'

#punctuation = u'\'\"!#$%\\()*+-/:;<>?@[]^_`}|}~'

def rep(n, x):
    s = ''
    for i in range(0, n):
        s += x

    return s

def loadKeywords():
    f = open('latinKeywords.txt', 'r')
    for line in f:
        line = line.lower()
        wordList.append( line.strip() )
    f.close()
    
    #f = open('arabicList.csv', 'r')
    #for line in f:
    #    wordList.append( line.strip().encode('utf-8') )
    #f.close()

def main():
    loadKeywords()    

    trans  = rep(len(string.punctuation), ' ')
    atrans = rep(len(arabicPunctuation), ' ')

    for line in sys.stdin:
        line = line.strip()

        try:
            data = json.loads(line)
        except ValueError as detail:
            continue
                
        if 'text' in data:
            # Parse data in the format of
            # Sat Mar 12 01:49:55 +0000 2011
            d  = string.split( data['created_at'], ' ')
            ds = ' '.join([d[1], d[2], d[3], d[5] ])
            dt = time.strptime(ds, '%b %d %H:%M:%S %Y')

            date = time.strftime('%Y-%m-%d %H:00:00', dt)

            ## turn text into lower case            
            if 'retweeted_status' in data:
                text = data['retweeted_status']['text'].lower()
            else:
                text = data['text'].lower()
            
            text = text.encode('utf-8')
            text = text.translate( string.maketrans(string.punctuation, trans) )
                            
            ## replace Arabic punctuation with spaces
            #for char in list(arabicPunctuation):        
            #    text = text.replace(char, u' ')

            words = text.split()
            
            for w in wordList:
                if w in words:
                    print '%s\t%s\t%s' % (date, w, 1)

if __name__ == '__main__':
    main()
    
