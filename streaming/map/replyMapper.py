#!/usr/local/bin/python2.7
#
# replyMapper.py
#
# Maps replies between specific individuals based if they are in a particular
# set of users. In this case, usually the users are individuals in our focused 
# samples.
#

import argparse, json, os, re, string, sys, time

users = {}

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

def formatDate(dateString):
    # Parse data in the format of Sat Mar 12 01:49:55 +0000 2011
    d  = string.split( dateString, ' ')
    ds = ' '.join([d[1], d[2], d[3], d[5] ])
    dt = time.strptime(ds, '%b %d %H:%M:%S %Y')
    return time.strftime('%Y-%m-%d %H:%M:%S', dt)

def loadUsers(levelFile):
    f = open(levelFile, 'r')
    for line in f:
        line = line.lower().strip()
        (user_id, level) = line.split("\t")

        if user_id == 'user_id':
            continue
        
        users[ user_id ] = level
    f.close()

def main():
#    parser.add_argument('-r', '--retweet', action = "store_true", 
#        help = "Include retweet information. Will output the same level of detail ")

    parser = argparse.ArgumentParser(description = 
        """Maps replies between specific individuals based if they are in a particular
        set of users. In this case, usually the users are individuals in our focused 
        samples.

        Its output is tweets either in raw JSON or a number of fields,
        so the reducer will probably just be an Identity function.""")

    parser.add_argument('-p','--primaryLevel', choices = ['1', '2', '3'], default = "1",
        help = "This is the set of users for which we would want to see the tweets of and replies to.")
    parser.add_argument('-s','--secondaryLevel', choices = ['1', '2', '3'],
        help = "This is the set of users for which we would want to see interactions between and the primary level.")
    parser.add_argument('--levelFile', default = "follow-all.txt",
        help = "File to use for tweet level information.")
    parser.add_argument('-t', '--tweetDetail', choices = ['low', 'medium', 'high'], default = 'low',
        help = """The level of detail in output. 'basic' includes status_id, timestamp, text, and basic user information.
        'moderate' includes more information, including user geolocation, user location, and user URL.
        'all' includes all available information in the tweet.""")
    parser.add_argument('-o', '--output', default="tab", choices = ['tab', 'JSON'],
        help = "Output format for the tweet.")

    args = parser.parse_args()

    loadUsers(args.levelFile)

    for line in sys.stdin:
        line = line.strip()

        try:
            data = json.loads(line)
        except ValueError as detail:
            continue
            
        if 'text' in data:
            uid       = None
            printThis = False

            in_reply_to_user_id_str = None

            if 'id_str' in data['user']:
                uid = data['user']['id_str']
            else:
                uid = str(data['user']['id'])

            in_reply_to_user_id_str = data['in_reply_to_user_id_str']

            ## print the original tweet if its from the group we're interested in
            if uid in users:
                if users[uid] == args.primaryLevel:
                    printThis = True
                elif args.secondaryLevel and users[uid] == args.secondaryLevel:
                    printThis = True
            elif in_reply_to_user_id_str in users:
                if users[in_reply_to_user_id_str] == args.primaryLevel:
                    printThis = True
                #elif args.secondaryLevel and users[in_reply_to_user_id_str] == args.secondaryLevel:
                #    printThis = True

            ## TK: Also need to take account of the fact that @replies to the primaryLevel
            if printThis:
                if not in_reply_to_user_id_str:
                    in_reply_to_user_id_str = '0'

                if not data['in_reply_to_status_id_str']:
                    reply = '0'
                else: 
                    reply = data['in_reply_to_status_id_str']

                toPrint = [data['id_str'], reply, in_reply_to_user_id_str,
                    (data['user']['screen_name'] + ": " + data['text'])]

                print "\t".join( map(validate, toPrint) )

                # ## print tab separated if specified
                # if args.output == 'JSON':
                #     print sid, "\t", line
                # else:
                #     ## TK: This stuff is all for search API
                #     # rt      = None
                #     # rt_user = None
                #     # rt_sn   = None

                #     # if 'retweeted_status' in data:
                #     #     if 'user' in data['retweeted_status']:
                #     #         rt_user = data['retweeted_status']['user']['name']
                #     #         rt_sn   = data['retweeted_status']['user']['screen_name']
                
                #     ## elements to print
                #     toPrint   = []

                #     ## tweets to process
                #     toProcess = [data]

                #     #if args.retweet and retweet:
                #     #    toProcess.append(retweet)

                #     for e in toProcess:
                #         coords = None

                #         ## calculate coordinates. specified in latitude then longitude.
                #         if e['coordinates']:
                #             coords = ",".join( map(str, reversed(e['coordinates']['coordinates'])) )                            
                #         elif e['geo']:
                #             coords = ",".join( map(str, e['geo']['coordinates']) )

                #         ## remove tabs and newlines from text
                #         e['text'] = e['text'].encode('utf-8')
                #         e['text'] = e['text'].translate( string.maketrans( '\t\n\r', '   ') )
                #         e['text'] = e['text'].decode('utf-8')

                #         ## print rather basic stuff
                #         if args.tweetDetail == 'low':
                #             u = e['user']
                #             toPrint.extend([
                #                 e['id_str'], 
                #                 formatDate( e['created_at'] ),
                #                 e['text'], 
                #                 u['id_str'], 
                #                 u['name'], 
                #                 u['screen_name'] 
                #             ])
                #         elif args.tweetDetail == 'medium':
                #             u = e['user']                            
                #             toPrint.extend([
                #                 e['id_str'], 
                #                 formatDate( e['created_at'] ),
                #                 e['text'],
                #                 e['source'],
                #                 e['geo'],
                #                 coords,
                #                 u['id_str'], 
                #                 u['name'], 
                #                 u['screen_name'],
                #                 u['description'],
                #                 u['location'],
                #                 u['url']
                #             ])
                #         elif args.tweetDetail == 'high':
                #             u = e['user']
                #             toPrint.extend([
                #                 e['id_str'], 
                #                 formatDate( e['created_at'] ),
                #                 e['text'],
                #                 e['source'],
                #                 coords,
                #                 u['id_str'], 
                #                 u['name'], 
                #                 u['screen_name'],
                #                 u['description'],
                #                 u['location'],
                #                 u['url'],
                #                 u['followers_count'],
                #                 u['friends_count'],
                #                 u['listed_count'],
                #                 u['statuses_count']
                #             ])

if __name__ == '__main__':
    main()

