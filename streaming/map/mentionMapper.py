#!/usr/local/bin/python2.7
# coding=utf-8
#
# mentionMapper.py
# This gets network info out of tweets.
#

import argparse, copy, json, re, string, sys, time, urllib2
from geopy import geocoders
from geopy.geocoders.googlev3 import *

def validate( x ):
    if x:
        if type(x) == unicode:
            x = string.replace(x, "\n", " ")
            x = string.replace(x, "\r", " ")
            x = string.replace(x, "\t", " ")
            return x.encode('utf-8')
        else:
            return str(x)
    else:
        return "0"

def loadKeywords(path):
    wordList = []
    f = open(path, 'r')
    for line in f:
        line = line.lower()
        wordList.append( line.strip() )
    f.close()
    return wordList

def getCoords(data, g, args):
    user = data['user']

    coords = None

    if 'coordinates' in data and data['coordinates']:
        coords = ",".join( map(str, reversed(data['coordinates']['coordinates'])) )                            
    elif 'geo' in data and data['geo']:
        coords = ",".join( map(str, data['geo']['coordinates']) )
    elif 'location' in user and user['location'] and user['location'] != "":
        loc = user['location'].lower().encode('utf-8').strip()

        ## some of these have exact locations
        if loc.startswith('iphone:') or loc.startswith('üt:'):
            coords = loc
        elif re.match(r"\-{0,1}[0-9]+.[0-9]+,\s*\-{0,1}[0-9]+.[0-9]+", loc):
            coords = loc
        elif args.dstk:
            try:
                place, (lat, lng) = g.geocode(user['location'])
                dst_coords = ",".join( map(str, [lat, lng]) )
            except (GQueryError, GTooManyQueriesError, ValueError, urllib2.HTTPError) as detail:
                #sys.stderr.write("Error: " + detail.__str__() + ": " + loc + "\n")
                pass
        #else:
        #   coords = loc

    return coords

def main():
    parser = argparse.ArgumentParser(description = "Mapper for generating user mentions and retweet network data from streaimng API tweets.")
    parser.add_argument('-d', '--date', choices = ['day','hour','minute'], 
        help = "Grouping by datetime. You can group by day, hour, minute, or none.")
    parser.add_argument('-k', '--keywordFile', default = None, 
        help = "Path of keyword file. If you are trying to group by particular keywords, note the ones you would like to use.")
    parser.add_argument('-g', '--geo', action = "store_true", help = "Add geolocation of source and target.")
    parser.add_argument('--dstk', help = """Name of the DataScienceToolKit server to use.
        You can use the www.datasciencetoolkit.org but it's probably more advisable to roll your own on Amazon S3.""")
    parser.add_argument('--printDate', action = "store_true")
    parser.add_argument('-r', '--rt', action = "store_true", help = "Only retweets.")
    parser.add_argument('-s', '--search', action = "store_true",
        help = "Structure of the tweet is slightly different in the search API.")

    args = parser.parse_args()

    if args.keywordFile:
        wordList = loadKeywords( args.keywordFile )

    ## set DSTK server
    if args.dstk:
        g = geocoders.GoogleV3(domain = args.dstk)

    for line in sys.stdin:
        line = line.strip()

        try:
            data = json.loads(line)
        except ValueError as detail:
            sys.stderr.write(detail.__str__() + "\n")
            continue

        if not (isinstance(data, dict)):
            ## not a dictionary, skip
            pass
        elif 'delete' in data:
            ## a delete element, skip for now.
            pass
        elif 'user' not in data:
            ## bizarre userless edge case
            pass
        else:
            toPrint = []
            user = data['user']

            ## search API does not have id_str
            if args.search:
                id_field = 'id'
            else:
                id_field = 'id_str'

            # Parse data in the format of Sat Mar 12 01:49:55 +0000 2011                
            d    = string.split( data['created_at'], ' ')
            ds   = ' '.join([d[1], d[2], d[3], d[5] ])
            dt   = time.strptime(ds, '%b %d %H:%M:%S %Y')
            date = None

            if args.date == 'day':
                date = time.strftime('%Y-%m-%d', dt)
            elif args.date == 'hour':
                date = time.strftime('%Y-%m-%d %H:00:00', dt)
            elif args.date == 'minute':
                date = time.strftime('%Y-%m-%d %H:%M:00', dt)

            if args.date:
                toPrint.append(date)

            ## check for keywords in text
            if args.keywordFile:
                printThis = False

                ## turn text into lower case            
                if 'retweeted_status' in data and data['retweeted_status']:
                    text = data['retweeted_status']['text'].lower()
                else:
                    text = data['text'].lower()
        
                ## encode text for unicode keywords
                text = text.encode('utf-8')

                ## copy the array for each word that appears
                for w in wordList:
                    if w in text:                    
                        printThis = True

                ## skip if the keyword is not in here
                if not printThis:
                    continue

            ## get the RT if it exists
            if 'retweeted_status' in data and data['retweeted_status']:
                orig = data['retweeted_status']

                if args.geo:
                    retweeted_coords = getCoords(data, g, args)
                    orig_coords      = getCoords(orig, g, args)

                    if orig_coords and retweeted_coords:
                        toPrint.extend(['retweet',
                            str(orig['user'][id_field]),
                            str(user[id_field])])

                        if args.printDate:
                            toPrint.extend([
                            orig['created_at'],
                            data['created_at']])
                                           
                        toPrint.extend([
                            orig_coords,
                            rt_coords,
                            "1"])

                        print "\t".join( map(validate, toPrint) )
                else:
                    toPrint.extend(['retweet',
                        str(orig['user'][id_field]),
                        str(user[id_field])])

                    if args.printDate:
                        toPrint.extend([
                        orig['created_at'],
                        data['created_at']])
                        
                    toPrint.extend(["1"])

                    print "\t".join( map(validate, toPrint) )
            else:
                if args.rt:
                    continue

                user_mentions = []
                if args.search:
                    if 'user_mentions' in data:
                        user_mentions = data['user_mentions']
                else:
                    if 'entities' in data and len(data['entities']['user_mentions']) > 0:
                        user_mentions = data['entities']['user_mentions']

                for u2 in user_mentions:
                    toPrintCopy = copy.copy(toPrint)
                    toPrintCopy.extend([
                        "user_mention",
                        str(user[id_field]),
                        str(u2[id_field]),
                        "1"
                        ])

                    print "\t".join( map(validate, toPrintCopy) )

if __name__ == '__main__':
    main()
