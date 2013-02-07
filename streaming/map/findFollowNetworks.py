#!/usr/bin/env python
## a distributed grep to generate relevant network lists

import sys

## follow[n][user_id] = level
follow = {}

def loadFollows():
    f = open('follow.txt', 'r')
    
    for line in f:
        line = line.strip()
    
        (user_id, level, round) = line.split('\t')

        if user_id == 'user_id':
            continue

        if round not in follow:
            follow[round] = {}

        follow[round][user_id] = level

def main(round):
    loadFollows()

    for line in sys.stdin:
        if len(line) == 0:
            break

        line = line.strip()

        user1, user2 = line.split('\t')        

        ## user1 is in level 2 and user2 is in level 1
        if (user1 in follow[round] and follow[round][user1] == '2' 
            and user2 in follow[round] and follow[round][user2] == '1'):
            print '\t'.join(user1, user2, '1')

        ## user1 is in level 3 and user2 is in level 2
        if (user1 in follow[round] and follow[round][user1] == '3'        
            and user2 in follow[round] and follow[round][user2] == '2'):        
            print '\t'.join(user1, user2, '2')

        ## user1 is in level 3 and user2 is in level 1
        if (user1 in follow[round] and follow[round][user1] == '3'        
            and user2 in follow[round] and follow[round][user2] == '1'):        
            print '\t'.join(user1, user2, '3')

if __name__ == '__main__':
    main( '1' )
    
