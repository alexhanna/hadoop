#!/usr/local/bin/python2.7
#
from operator import itemgetter
import sys

json      = {}
accessed  = {}
repliesTo = {}

def printStatus(id):
	## base case
	if id in repliesTo and repliesTo[id] == '0':
		if id in json:
			print json[id]

			json[id] = None
		return
	else:
		if id in repliesTo:
			printStatus( repliesTo[id] )

			if repliesTo[id] in json:
				print json[ repliesTo[id] ]
				json[ repliesTo[id] ] = None

def main():
	for line in sys.stdin:
		line = line.strip()
		try:
			(status_id, in_reply_to_status_id_str, jsonStr) = line.split("\t")
		except ValueError as detail:
			print detail
			print line
			return

		json[status_id] = jsonStr

		if in_reply_to_status_id_str:
			repliesTo[status_id] = in_reply_to_status_id_str

	for id in reversed(repliesTo.keys()):
		replyChain    = []
		accessedChain = []

		## if this status is a reply
		if repliesTo[id]:
			curr_id = id

			## if it is not, follow the reply chain if we have the tweet

			## TK: need to check somehow for chains that have already been produced.
			while curr_id != None and curr_id in json:
				## append the early tweets to the front of the chain
				replyChain.insert(0, json[curr_id])
				if curr_id in accessed:
					accessed[curr_id] += 1
				else:
					accessed[curr_id] = 1
				accessedChain.append( accessed[curr_id] )

				curr_id = repliesTo[curr_id]

		if len(replyChain) > 2:
			## if there is at least one new message in the chain, print it
			if 1 in accessedChain:
				print "\n".join(replyChain)
				print "----"


if __name__ == '__main__':
    main()