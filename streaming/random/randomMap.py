#!/usr/bin/env python

import json, string, sys, time

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
    f = open('follow-r' + wave + '.txt', 'r')
    for line in f:
        line = line.lower().strip()
        (user_id, level) = line.split("\t")

        if user_id == 'user_id':
            continue
        
        users[ user_id ] = level 
    f.close()


def main():
    #    levels = {}

    ## open file for current round
    #f = open('follow-r3.txt', 'r')
    #for line in f:
    #    line = line.strip()
    #    (user, level) = string.split(line, "\t")

    #   levels[user] = level

    for line in sys.stdin:
	line = line.strip()
	data = ''

        try:
            data = json.loads(line)
        except ValueError as detail:
            continue
        
        if 'id' in data:
            user = data['user']            
            uid  = user['id_str']
            
            ## get second level tweets
            # if uid in levels and levels[uid] == '2':
                # Parse date
            d  = string.split( data['created_at'], ' ')
            ds = ' '.join([d[1], d[2], d[3], d[5] ])
            dt = time.strptime(ds, '%b %d %H:%M:%S %Y')

            date = time.strftime('%Y-%m-%d %H:%M:%S', dt)

            rt = ''
            if 'retweeted_status' in data:
                rt = data['retweeted_status']['text']

            ## items to print
            toPrint = [
                data['id'],
                date,
                user['name'],
                user['screen_name'],
                user['followers_count'],
                user['friends_count'],
                user['statuses_count'],
                data['text'],
                rt
                ]

            print "\t".join( map(validate, toPrint) )
                
if __name__ == '__main__':
    main()
