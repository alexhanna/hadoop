#!/usr/bin/env python

import json, string, sys, time

def validate( x ):
    if x:
        return str(x)
    else:
        return "0"

def main( c_round ):
    users  = {}
    levels = {}

    ## open file for current round
    f = open('follow-r' + c_round + '.txt', 'r')
    for line in f:
        line = line.strip()
        (user, level) = string.split(line, "\t")
        
        users[user]  = 1
        levels[user] = level
        
    f.close()

    for line in sys.stdin:
        line = line.strip()

        data = ''
        try:
            data = json.loads(line)
        except ValueError as detail:
            sys.stderr.write(line + "\n")
            sys.stderr.write(detail.__str__() + "\n")
            continue

        # Parse data in the format of
        # Sat Mar 12 01:49:55 +0000 2011
        d  = string.split( data['created_at'], ' ')
        ds = ' '.join([d[1], d[2], d[3], d[5] ])
        dt = time.strptime(ds, '%b %d %H:%M:%S %Y')

        date = time.strftime('%Y-%m-%d %H:%M:%S', dt)

        user   = data['user']
        id_str = str(user['id'])

        if id_str in users:
            toPrint = [
                id_str,
                user['screen_name'],
                levels[ id_str ],
                date, 
                user['followers_count'],
                user['friends_count'],
                user['statuses_count'],
                user['listed_count']
                ]

            toPrint = map( validate, toPrint )

            print "\t".join( toPrint )
                        
if __name__ == '__main__':
    main( "3" )
