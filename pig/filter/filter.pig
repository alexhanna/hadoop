-- REGISTER parsing jars
REGISTER ../../jar/json-simple-1.1.1.jar;
REGISTER ../../jar/guava-13.0.1.jar;
REGISTER ../../jar/elephant-bird-pig-3.0.0.jar;
REGISTER ../../jar/myudfs.jar;
REGISTER /usr/lib/pig/contrib/piggybank/java/piggybank.jar;

DEFINE ToDate myudfs.ToDate();
DEFINE ToDatetimeByHour myudfs.ToDatetimeByHour();
DEFINE REGEX_EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtract();

-- Load the JSON
--raw = LOAD 'elextest' USING com.twitter.elephantbird.pig.load.JsonLoader() AS (json: map[]);
raw = LOAD 'elextest' USING com.twitter.elephantbird.pig.load.JsonLoader() AS (json: map[]);

A = FILTER raw BY (LOWER(json#'text') MATCHES '.*($keywords).*');
B = FOREACH A GENERATE ToDatetimeByHour( json#'created_at' ) AS timestamp, REGEX_EXTRACT( LOWER(json#'text'), '.*($keywords).*', 1) AS keyword;
C = GROUP B BY (timestamp, keyword);
D = FOREACH C GENERATE group, COUNT(B) AS count;
E = FOREACH D GENERATE FLATTEN($0) AS (timestamp:chararray, keyword:chararray), count;
out = ORDER E BY timestamp PARALLEL 1;

STORE out INTO 'output' USING PigStorage('\t');
