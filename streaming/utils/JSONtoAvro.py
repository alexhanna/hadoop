from avro import schema, datafile, io
import json, sys
from types import *

def main():
	if len(sys.argv) < 2:
		print "Usage: cat input.json | python2.7 JSONtoAvro.py output"
		return

	s = schema.parse(open("tweet.avsc").read())
	f = open(sys.argv[1], 'wb')

	writer = datafile.DataFileWriter(f, io.DatumWriter(), s, codec = 'deflate')

	failed = 0

	for line in sys.stdin:
		line = line.strip()

		try:
			data = json.loads(line)
		except ValueError as detail:
			continue

		try:
			writer.append(data)
		except io.AvroTypeException as detail:
			print line
			failed += 1

	writer.close()

	print str(failed) + " failed"

if __name__ == '__main__':
	main()
