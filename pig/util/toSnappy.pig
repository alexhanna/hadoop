set mapred.compress.map.output true;
set mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

REGISTER /project/hanna/src/json-simple-1.1.1.jar;
REGISTER /project/hanna/src/guava-13.0.1.jar;
REGISTER /usr/lib/pig/contrib/piggybank/java/lib/snappy-java-1.0.3.2.jar;
REGISTER /project/hanna/src/elephant-bird/build/elephant-bird-2.2.0.jar;

raw = LOAD 'test.json' USING com.twitter.elephantbird.pig.load.JsonLoader() AS (json: map[]);

DUMP raw;