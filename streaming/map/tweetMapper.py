#!/usr/local/bin/python2.7
#
# tweetMapper.py
# This script gets tweets based on user levels and keywords 
# Output is a number of tweet options, but can also be raw JSON
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
    ## TK: implement getOpts here
    ## do switches for keywords and users
    ## these are temporary, for testing

    trans = rep(len(string.punctuation), ' ')

    parser = argparse.ArgumentParser(description = 
        """This script operates as the mapper part of MapReduce job. 
        Its output is tweets either in raw JSON or a number of fields,
        so the reducer will probably just be an Identity function.""")
    parser.add_argument('-a', '--all', action = "store_true", help = "Print out all tweets, no constraints")
    parser.add_argument('-k', '--keywordFile', default = None, help = "Keywords file")
    parser.add_argument('--userLevels', default = None)
    parser.add_argument('--wave', default = 3)
    parser.add_argument('-o', '--output', default="tab", choices = ['tab', 'JSON'],
        help = "Output format for the tweet.")


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
            user = data['user']            
            uid  = None
            sid  = None
            printThis = False
            
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
                printThis = True

            if args.keywordFile:
                printThis = False
                testText = ''
                ## turn text into lower case            
                if 'retweeted_status' in data:
                    testText = data['retweeted_status']['text'].lower()
                else:
                    testText = data['text'].lower()
        
                testText = testText.encode('utf-8')
                testText = testText.translate( string.maketrans(string.punctuation, trans) )

                words = testText.split()
        
                for w in wordList:
                    if w in words:
                        printThis = True
                        break

            if args.all:
                printThis = True 

            ## Print if all the prior conditions have been met
            if printThis:
                ## print tab separated if specified
                if args.output == 'JSON':
                    print sid, "\t", line
                else:
                    # Parse data in the format of Sat Mar 12 01:49:55 +0000 2011
                    # d  = string.split( data['created_at'], ' ')
                    # ds = ' '.join([d[1], d[2], d[3], d[5] ])
                    # dt = time.strptime(ds, '%b %d %H:%M:%S %Y')

                    # date = time.strftime('%Y-%m-%d %H:%M:%S', dt)

                    ## items to print
                    toPrint = [
                        data['id'],
                        data['created_at'],
                        user['name'],
                        user['screen_name'],
                        data['text'],
                        rt,
                        rt_user,
                        rt_sn
                        ]

                    print "\t".join( map(validate, toPrint) )

                    

if __name__ == '__main__':
    main()
    
