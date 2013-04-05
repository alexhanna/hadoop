#!/usr/local/bin/python2.7
#
from operator import itemgetter
import argparse, sys

json          = {}
accessed      = {}
repliesTo     = {}
userRepliesTo = {}
users         = {}

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
    parser = argparse.ArgumentParser(description = "Reducer for replies.")
    parser.add_argument('--levelFile', default = "follow-all.txt", help = "File to use for tweet level information.")

    args = parser.parse_args()
    loadUsers(args.levelFile)

    for line in sys.stdin:
        line = line.strip()
        try:
            (status_id, in_reply_to_status_id_str, in_reply_to_user_id_str, jsonStr) = line.split("\t")
        except ValueError as detail:
            print detail
            print line
            return

        json[status_id] = jsonStr

        if in_reply_to_status_id_str:
            repliesTo[status_id] = in_reply_to_status_id_str
            userRepliesTo[status_id] = in_reply_to_user_id_str

    for id in reversed(repliesTo.keys()):
        replyChain     = []
        accessedChain  = []
        userLevelChain = []

        ## if this status is a reply
        if repliesTo[id]:
            curr_id = id            

            ## if it is not, follow the reply chain if we have the tweet
            while curr_id != None and curr_id in json:
                ## append the early tweets to the front of the chain
                replyChain.insert(0, json[curr_id])
                if curr_id in accessed:
                    accessed[curr_id] += 1
                else:
                    accessed[curr_id] = 1

                ## note that this chain has been accessed.
                accessedChain.append( accessed[curr_id] )

                if userRepliesTo[curr_id] not in users:
                    pass
                else:
                    ## put the user level in the chain
                    userLevelChain.append( users[ userRepliesTo[curr_id] ] )

                curr_id = repliesTo[curr_id]

        if len(replyChain) > 2:
            ## if there is at least one new message in the chain, print it
            if 1 in accessedChain:
                ## if there is at least one level 1 user in the chain
                if "1" in userLevelChain:
                    print "\n".join(replyChain)
                    print "----"

if __name__ == '__main__':
    main()