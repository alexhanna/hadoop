#!/usr/bin/python
# coding=utf-8

# locationMapper.py
#  
# This script extracts location information from tweets using the Google
# geocode API and some stuff from the DataScienceToolKit.
# This is mostly a test of what I can do with location stuff.
# 

import json, os, re, string, sys, time
from geopy import geocoders

def main():
    total = 0
    coded = 0

    for line in sys.stdin:
        total += 1

        line = line.strip()

        try:
            data = json.loads(line)
        except ValueError as detail:
            continue

        if not (isinstance(data, dict)):
            ## not a dictionary, skip
            pass
        elif 'delete' in data:
            ## a delete element, skip for now.
            pass
        elif 'user' not in data:
            ## bizarre userless edge case
            pass
        else:
            user    = data['user']
            uid     = None
            toPrint = []
            
            # Parse data in the format of Sat Mar 12 01:49:55 +0000 2011                
            d    = string.split( data['created_at'], ' ')
            ds   = ' '.join([d[1], d[2], d[3], d[5] ])
            dt   = time.strptime(ds, '%b %d %H:%M:%S %Y')
            date = None

            timemode = 'hour'

            if timemode == 'day':
                date = time.strftime('%Y-%m-%d', dt)
            elif timemode == 'hour':
                date = time.strftime('%Y-%m-%d %H:00:00', dt)
            elif timemode == 'minute':
                date = time.strftime('%Y-%m-%d %H:%M:00', dt)
            
            toPrint.append(date)

            coords = None
            loc    = None

            ## calculate coordinates. specified in latitude then longitude.
            if data['coordinates']:
                coords = ",".join( map(str, reversed(data['coordinates']['coordinates'])) )                            
            elif data['geo']:
                coords = ",".join( map(str, data['geo']['coordinates']) )
            elif user['location'] != "" and user['location'] != None:
                loc = user['location'].lower().encode('utf-8').strip()

                ## some of these have exact locations
                if loc.startswith('iphone:') or loc.startswith('üt:'):
                    coords = loc.split(" ")[1]
                    continue

                ## change this as appropriate
                g = geocoders.GoogleV3(domain = "ec2-50-16-111-255.compute-1.amazonaws.com")

                ## TK: Build some cache or something that I can store locally
                ## so I don't have to hit the server so often

                try:
                    place, (lat, lng) = g.geocode(loc)
                    coords = ",".join( map(str, [lat, lng]) )
                except Exception as detail:      
                    print detail              
                    pass

            if coords:
                toPrint.append(coords)
                print "\t".join(toPrint)
                #print user['location']
                coded += 1
            elif loc:
                pass
                #toPrint.append(user['location'])
                #print "\t".join(toPrint)                

    print "%s coded out of %s" % (coded, total)

if __name__ == '__main__':
    main()
