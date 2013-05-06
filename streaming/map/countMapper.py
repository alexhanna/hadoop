#!/usr/local/bin/python2.7
#
# countMapper.py
# This script gets counts of tweets based on user levels and keywords
# Output is the specified keys and aggregated counts
# 

import argparse, json, os, re, string, sys, time

users    = {}
wordList = []

def rep(n, x):
    s = ''
    for i in range(0, n):
        s += x

    return s

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

def loadUsers(levelFile):
    f = open(levelFile, 'r')
    for line in f:
        line = line.lower().strip()
        (user_id, level) = line.split("\t")

        if user_id == 'user_id':
            continue
        
        users[ user_id ] = level 
    f.close()

def loadKeywords(path):
    f = open(path, 'r')
    for line in f:
        line = line.lower()
        wordList.append( line.strip() )
    f.close()

def main():
    punc  = '.,-'
    trans = rep(len(punc), ' ')

    parser = argparse.ArgumentParser(description = 
        """This script operates as the mapper part of MapReduce job. 
        Its output is a count of messages that meet the criteria which 
        are set in the options. The reducer is usually a simple sum function.""")
    parser.add_argument('-d', '--date', choices = ['day','hour','minute'], 
        help = "Grouping by datetime. You can group by day, hour, or minute.")
    parser.add_argument('-g', '--geo', action = "store_true", 
        help = "Grouping by geolocation. Rounding to the first decimal point.")
    parser.add_argument('-k', '--keywordFile', default = None, 
        help = "Path of keyword file. If you are trying to group by particular keywords, note the ones you would like to use.")
    parser.add_argument('-l','--level', choices = ['1', '2', '3' ,'all'])
    parser.add_argument('--levelFile', default = "follow-all.txt")
    parser.add_argument('--minUserFollowers', type = int,
        help = "Minimum number of followers that the user should have.")
    parser.add_argument('--minUserTweets', type = int,
        help = "Minimum number of tweets that the user should have.")
    parser.add_argument('--user', action = "store_true",
        help = "Grouping by user id.")

    args = parser.parse_args()

    if args.keywordFile:
        loadKeywords( args.keywordFile )

    if args.level:
        loadUsers(args.levelFile)

    for line in sys.stdin:
        line = line.strip()

        try:
            data = json.loads(line)
        except ValueError as detail:
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
            user    = data['user']
            uid     = None
            toPrint = []
            
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

            if 'id_str' in user:
                uid = user['id_str']
            else:
                uid = str(user['id'])

            ## append user id
            if args.user:
                toPrint.append(uid)

            ## append user level
            if args.level:
                if uid in users and (args.level == 'all' or users[uid] == args.level):
                    toPrint.append(users[uid])
                else:
                    continue

            ## skip this tweet if user does not meet minimum number of followers
            if args.minUserFollowers:
                if user['followers_count'] < args.minUserFollowers:
                    continue

            ## skip this tweet if user does not meet minimum number of tweets
            if args.minUserTweets:
                if user['statuses_count'] < args.minUserTweets:
                    continue

            if args.geo:
                if data['coordinates']:
                    coords = data['coordinates']['coordinates']

                    ## reversing so it is latitude, then longitude
                    coordStr = ",".join([
                        str(round(coords[1], 1)),
                        str(round(coords[0], 1))
                        ])

                    toPrint.append(coordStr)
                else:
                    toPrint.append('0')

            ## for keywords, need to handle this a little different because we want this to
            ## print every time there is an instance of the word
            if args.keywordFile:
                ## turn text into lower case            
                if 'retweeted_status' in data and data['retweeted_status']:
                    text = data['retweeted_status']['text'].lower()
                else:
                    text = data['text'].lower()
        
                ## encode text for unicode keywords
                text = text.encode('utf-8')

                ## copy the array for each word that appears
                for w in wordList:
                    ## this is a rule with a boolean
                    ## of the form kw1 & kw2 & ... kwN
                    if "&" in w:
                        rules = w.split(" & ")
                        for r in rules:
                            ## must meet all the rules
                            if r not in text:
                                break
                        else:
                            toPrintCopy = list(toPrint)
                            toPrintCopy.append(w)
                            toPrintCopy.append('1')

                            print "\t".join(toPrintCopy)
                    elif w in text:
                        toPrintCopy = list(toPrint)
                        toPrintCopy.append(w)
                        toPrintCopy.append('1')                        

                        print "\t".join(toPrintCopy)
            else:
                toPrint.append('1')
                print "\t".join(toPrint)

                    

if __name__ == '__main__':
    main()
    
