#!/usr/bin/env python
## this categorizes top-level users RTs

import json, sys

## follow[user_id] = level
follow   = {}
category = {}

def loadCategories():
    f = open('top-level-uid_cat.csv', 'r')
    for line in f:
        line = line.strip()
        (user_id, cat) = line.split("\t")
        
        category[ user_id ] = cat

def loadFollows():
    f = open('follow-all.txt', 'r')
    
    for line in f:
        line = line.strip()
        (user_id, level) = line.split('\t')

        if user_id == 'user_id':
            continue
        else:
            follow[user_id] = int(level)

def main():
    loadFollows()
    loadCategories()

    for line in sys.stdin:
        line = line.strip()

        data = ''
        try:
            data = json.loads(line)
        except ValueError as detail:
            sys.stderr.write(line + "\n")
            sys.stderr.write(detail.__str__() + "\n")
            continue

        user = data['user']        

        ## if this is a RT
        ## TK: Change this to incorporate informal RTs
        if 'retweeted_status' in data:
                rtUser = data['retweeted_status']['user']

                ## if RTing user is in the top-level
                if user['id_str'] in follow and follow[ user['id_str'] ] == 1:
                        if rtUser['id_str'] in follow:
                                ## if this is on the top level, emit categories
                                if follow[ rtUser['id_str'] ] == 1:
                                        print "\t".join([ category[ user['id_str'] ],
                                                          category[ rtUser['id_str'] ],
                                                          "1"
                                                ])                        
                                elif follow[ rtUser['id_str'] ] == 2:
                                        print "\t".join([ category[ user['id_str'] ],
                                                          "level2",
                                                          "1"
                                                ])
                        else:
                                print "\t".join([ category[ user['id_str'] ],
                                                  "other",
                                                  "1"
                                        ])

if __name__ == '__main__':
    main( )
