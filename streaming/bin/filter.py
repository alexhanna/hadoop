#!/usr/bin/env python

import nltk, json
import re, string, sys

latinList   = []
unicodeList = []

def loadKeywords():
    f = open('latinKeywords.txt', 'r')
    for line in f:
        latinList.append( line.strip() )
    f.close()

    if os.path.exists('unicodeKeywords.txt'):
        f = open('unicodeKeywords.txt', 'r')
        for line in f:
            unicodeList.append( line.strip() )
        f.close()

def main():
    loadKeywords()

    ## ignore case for Latin
    latinRe   = re.compile( string.join(latinList, '|'), re.I | re.M )
    unicodeRe = re.compile( string.join(unicodeList, '|'), re.U | re.M )
    
    while True:
        line = sys.stdin.readline()
        if len(line) == 0:
            break

        ## apply text transforms
        v = line.strip()

        data = ''
        try:
            data = json.loads(v)
        except ValueError as detail:
            sys.stderr.write(v + "\n")
            sys.stderr.write(detail.__str__() + "\n")
            continue
                
        if 'text' in data:
            text = data['text']            
            rt   = ''
            if 'retweeted_status' in data:
                rt = data['retweeted_status']['text']
            
            if latinRe.search(text) or unicodeRe.search(text):
                print v
            elif rt != '' and (latinRe.search(rt) or unicodeRe.search(rt)):
                print v
            else:
                data = 0

if __name__ == '__main__':
    main()
    
