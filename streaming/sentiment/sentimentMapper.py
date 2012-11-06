#!/usr/bin/env python

from __future__ import division
import json, string, sys, time

sentimentDict = {
    'positive': [],
    'negative': []
}


def loadSentiment():
    f = open('../data/carenPos.txt', 'r')
    for line in f:
        sentimentDict['positive'].append(line.strip())
    f.close()

    f = open('../data/carenNeg.txt', 'r')
    for line in f:
        sentimentDict['negative'].append(line.strip())
    f.close()

def main():
    loadSentiment()
    
    for line in sys.stdin:
        line = line.strip()

        data = ''
        try:
            data = json.loads(line)
        except ValueError as detail:
            sys.stderr.write(detail.__str__() + "\n")
            continue

        if 'text' in data:
            # Parse data in the format of
            # Sat Mar 12 01:49:55 +0000 2011
            d  = string.split( data['created_at'], ' ')
            ds = ' '.join([d[1], d[2], d[3], d[5] ])
            dt = time.strptime(ds, '%b %d %H:%M:%S %Y')

            date = time.strftime('%Y-%m-%d %H:%M:00', dt)

            msg = data['text']

            ## turn text into lower case
            text   = msg.lower()

            ## encode in UTF-8 to get rid of Unicode errors
            #text   = text.encode('utf-8')
            #text   = text.translate( string.maketrans("",""), string.punctuation )

            for p in list(string.punctuation):                                
                text = text.replace(p, ' ')

            words  = text.split(' ')
            lwords = len(words)

            counts = {
                'positive':0,
                'negative':0
                }

            ratios = {
                'positive':0,
                'negative':0
            }

            for c in ['obama', 'romney']:
                if c in text:
                    for a in ['positive', 'negative']:
                        for w in sentimentDict[a]:
                            if w in words:
                                counts[a] += 1

                        ratios[a] = counts[a]/lwords

                    ## calculate overall sentiment by subtracting one from another 
                    print "\t".join([date, c, str(ratios['positive'] - ratios['negative'])])                              
if __name__ == '__main__':
    main()
