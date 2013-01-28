-- REGISTER parsing jars
REGISTER ../../jar/json-simple-1.1.1.jar;
REGISTER ../../jar/guava-13.0.1.jar;
REGISTER ../../jar/elephant-bird-pig-3.0.0.jar;
REGISTER ../../jar/myudfs.jar;
REGISTER /usr/lib/pig/contrib/piggybank/java/piggybank.jar;

DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap();

-- Load the JSON
--raw = LOAD '../data/test.json' USING com.twitter.elephantbird.pig.load.JsonLoader() AS (json: map[]);
raw = LOAD 'elextest' USING com.twitter.elephantbird.pig.load.JsonLoader() AS (json: map[]);
--rawdata = LOAD 'test.json.bz2' AS (r: chararray);
--raw = FOREACH rawdata GENERATE JsonStringToMap(r) AS json;

A = SAMPLE raw 0.01;
B = FOREACH A GENERATE json#'id' AS id, 
	json#'created_at',
--	JsonStringToMap(json#'user'),
	REPLACE(json#'text', '\n', ' ') AS text;
--C = ORDER B BY id DESC PARALLEL 1;

DUMP B;
--STORE B INTO 'output' USING PigStorage('\t');
