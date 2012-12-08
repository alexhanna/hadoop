#!/usr/bin/env python

import json, re, sys

def main():
    for line in sys.stdin:
        line = line.strip()

        if len(line) == 0:
          continue

        ## fix for janky MySQL escaping        
        line = re.sub(r'\\(.)', r'\1', line)
        line = line[:-1]

        ## TK: Something weird is happening with newlines here

        data = ''
        try:
            data = json.loads(line)
        except ValueError as detail:
            #sys.stderr.write(detail.__str__() + "\n")
            continue

        if 'id' in data:
          ## get the tweet ID
       	  id = data['id']

       	  ## print the ID and the JSON
       	  print "\t".join([ str(id), line])

if __name__ == '__main__':
    main()