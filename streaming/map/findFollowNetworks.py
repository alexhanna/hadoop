#!/usr/local/bin/python2.7
#
## a distributed grep to generate relevant network lists
#

import argparse, sys

users = {}

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
    parser = argparse.ArgumentParser(description = 
        """This script is effectively a distributed grep which gets
        the relational data for people who are in our focused sample.""")
    parser.add_argument('-l','--levelFile', default = 'follow-r1.txt',
        help = "The wave that we want to consider.")

    args = parser.parse_args()

    loadUsers(args.levelFile)

    for line in sys.stdin:
        if len(line) == 0:
            break

        line = line.strip()

        ## Structure of the relation file is user2 -> user1
        user1, user2 = line.split('\t')        

        ## Outputing follow networks in the same order
        ## for standardizaion

        if user1 in users and user2 in users:

            # Level 2 (user2) follows level 1 (user1)
            if users[user1] == '1' and users[user2] == '2':
                print '\t'.join([user1, user2, '2to1'])

            # Level 3 (user2) follows level 2 (user1)
            if users[user1] == '2' and users[user2] == '3':            
                print '\t'.join([user1, user2, '3to2'])

            # Level 3 (user2) follows level 1 (user1)
            if users[user1] == '1' and users[user2] == '3':
                print '\t'.join([user1, user2, '3to1'])

if __name__ == '__main__':
    main()
    
