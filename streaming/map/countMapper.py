#!/usr/local/bin/python2.7
#
# countMapper.py
# This script gets counts of tweets based on user levels and keywords
# Output is the specified keys and a number
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
            return x.encode('utf-8')
        else:
            return str(x)
    else:
        return "0"

def loadUsers(wave):
    f = open('follow-r' + str(wave) + '.txt', 'r')
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
    parser.add_argument('-k', '--keywordFile', default = None, 
        help = "Path of keyword file. If you are trying to group by particular keywords, note the ones you would like to use.")
    parser.add_argument('--userLevels', choices = ['1', '2', '3' ,'all'], default = None)
    parser.add_argument('--wave', default = None)

    args = parser.parse_args()

    if args.keywordFile:
        loadKeywords( args.keywordFile )

    if args.userLevels == 'all':
        loadUsers(args.wave)

    for line in sys.stdin:
        line = line.strip()

        try:
            data = json.loads(line)
        except ValueError as detail:
            continue
                
        if 'text' in data:
            user    = data['user']            
            uid     = None
            sid     = None
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

            toPrint.append(date)

            if 'id_str' in data:
                sid = data['id_str']
            else:
                sid = str(data['id'])

            if 'id_str' in user:
                uid = user['id_str']
            else:
                uid = str(user['id'])

            text = data['text']

            ## turn text into lower case
            rt      = None
            rt_user = None
            rt_sn   = None

            if 'retweeted_status' in data:
                rt = data['retweeted_status']['text']

                if 'user' in data['retweeted_status']:
                    rt_user = data['retweeted_status']['user']['name']
                    rt_sn   = data['retweeted_status']['user']['screen_name']

            if args.userLevels == 'all' or (uid in users and users[uid] in args.userLevels):
                toPrint.append(users[uid])

            ## for keywords, need to handle this a little different because we want this to
            ## print every time there is an instance of the word
            if args.keywordFile:
                testText = ''
                ## turn text into lower case            
                if 'retweeted_status' in data:
                    testText = data['retweeted_status']['text'].lower()
                else:
                    testText = data['text'].lower()
        
                testText = testText.encode('utf-8')
                testText = testText.translate( string.maketrans(punc, trans) )

                words = testText.split()

                ## copy the array for each word that appears
                toPrintCopy = list(toPrint)
                for w in wordList:
                    if w in words:
                        toPrintCopy.append(w)
                        toPrintCopy.append('1')

                        print "\t".join(toPrintCopy)
            else:
                toPrint.append('1')
                print "\t".join(toPrint)

                    

if __name__ == '__main__':
    main()
    
