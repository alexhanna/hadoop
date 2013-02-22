from avro import schema, datafile, io
import json, sys
from types import *

def main():
	s = schema.parse(open("tweet.avsc").read())

	writer = datafile.DataFileWriter(sys.stdout, io.DatumWriter(), s, codec = 'deflate')

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
			sys.stderr.write(line + "\n")
			failed += 1

	#writer.close()

	sys.stderr.write(str(failed) + " failed\n")

if __name__ == '__main__':
	main()
