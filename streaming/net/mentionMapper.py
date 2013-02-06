#!/usr/bin/env python

import json, string, sys, time

search = False

def main():

    ## if this is from search API, use different field for mentions
    if len(sys.argv) > 0 and sys.argv[1] == '1':
        search = True

    for line in sys.stdin:
        line = line.strip()

        data = ''
        try:
            data = json.loads(line)
        except ValueError as detail:
            sys.stderr.write(detail.__str__() + "\n")
            continue

        if (search and 'user_mentions' in data) or ('entities' in data and len(data['entities']['user_mentions']) > 0):
            user = data['user']

            if search:
                user_mentions = data['user_mentions']
            else:
                user_mentions = data['entities']['user_mentions']

            d  = string.split( data['created_at'], ' ')
            ds = ' '.join([d[1], d[2], d[3], d[5] ])
            dt = time.strptime(ds, '%b %d %H:%M:%S %Y')

            date = time.strftime('%Y-%m-%d %H:00:00', dt)

            ## search API does not have id_str
            id_field = 'id_str'
            if search:
                id_field = 'id'

            for u2 in user_mentions:                
                print "\t".join([
                    date, 
                    str(user[id_field]),
                    str(u2[id_field]),
                    "1"
                    ])

if __name__ == '__main__':
    main()
