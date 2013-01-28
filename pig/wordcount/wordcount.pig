-- REGISTER parsing jars
REGISTER /project/hanna/src/json-simple-1.1.1.jar;
REGISTER /project/hanna/src/guava-13.0.1.jar;
REGISTER /project/hanna/src/elephant-bird/build/elephant-bird-2.2.0.jar;

-- Load the JSON
json = LOAD '/user/ahanna/elextest/*.json' USING com.twitter.elephantbird.pig.load.JsonLoader();

words  = FOREACH json GENERATE FLATTEN(TOKENIZE( (chararray) $0#'text')) as word;
groups = GROUP words by word;
counts = FOREACH groups GENERATE COUNT(words), group;
orders = ORDER counts by group;

STORE orders INTO 'output' USING PigStorage('\t');
