#!/usr/bin/env python
## a distributed grep to generate categorization for level 2 users

import sys

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
    f = open('follow-r1.txt', 'r')
    
    for line in f:
        line = line.strip()
        (user_id, level) = line.split('\t')

        if user_id == 'user_id':
            continue
        else:
            follow[user_id] = level

def main():
    loadFollows()
    loadCategories()

    for line in sys.stdin:
        line = line.strip()

        ## user1 is the level above user2
        user1, user2 = line.split('\t')

        if (user1 in follow and follow[user1] == '1' 
            and user2 in follow and follow[user2] == '2'):
            print '\t'.join([user2, category[user1], '1'])

if __name__ == '__main__':
    main( )
