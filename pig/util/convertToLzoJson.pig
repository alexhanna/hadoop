-- REGISTER parsing jars
REGISTER /project/hanna/src/json-simple-1.1.1.jar;
REGISTER /project/hanna/src/guava-13.0.1.jar;
REGISTER /project/hanna/src/elephant-bird/build/elephant-bird-2.2.0.jar;

-- use only one reducer
set default_parallel 1;

-- Load the JSON
raw = LOAD 'elextest' USING com.twitter.elephantbird.pig.load.JsonLoader() AS (json: map[]);
STORE raw INTO 'lzotest2' USING com.twitter.elephantbird.pig.store.LzoJsonStorage();
