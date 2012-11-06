#!/usr/bin/env python

import sys

ckey  = None
cval  = 0
cdate = None

for line in sys.stdin:
    line = line.strip()

    inputWords = line.split("\t")

    key  = "\t".join( inputWords[0:3] )
    date = inputWords[3]
    val  = "\t".join( inputWords[4:] )

    ## Hadoop orders things in sorted order
    ## so we can just take the last item in the list
    if cdate <= date:
        cdate = date
        cval  = val 
        ckey  = key
    else:
        if ckey:
            print "%s\t%s" % (ckey, cval)

        cdate = date
        ckey  = key
        cval  = val

if ckey == key:
    print "%s\t%s" % (ckey, cval)
