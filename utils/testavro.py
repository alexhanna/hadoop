#!/usr/bin/env python

from avro.io import validate
from avro.schema import parse
from json import loads
from sys import argv

def main(argv):
    valid = set()
    invalid_avro = set()
    invalid_json = set()

    if len(argv) < 3:
        print "Give me an avro schema file and a whitespace-separated list of json files to validate against it."
    else:
        schema = parse(open(argv[1]).read())
        for arg in argv[2:]:
            try:
                json = loads(open(arg, 'r').read())
                if validate(schema, json):
                    valid.add(arg)
                else:
                    invalid_avro.add(arg)
            except ValueError:
                invalid_json.add(arg)
    print 'Valid files:\n\t' + '\n\t'.join(valid)
    print 'Invalid avro:\n\t' + '\n\t'.join(invalid_avro)
    print 'Invalid json:\n\t' + '\n\t'.join(invalid_json)

if '__main__' == __name__:
    main(argv)