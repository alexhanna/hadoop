#!/usr/bin/env python

import csv, os, re, string, sys, time

def main():
    ## Put keywords that you would like here
    latinList = ['Egypt']

    print "numerical_ID,local_date,time,tweet_user,tweet_family,ddqual_TweetBody,tweet_type,media_links"

    with sys.stdin as csvfile:
        f = csv.reader(sys.stdin)    
        for row in f:
            msg = row[5].lower()
            
            for word in latinList: 
                word = word.lower()
                if word in msg:
                    print ",".join(row)

if __name__ == '__main__':
    main()
    
