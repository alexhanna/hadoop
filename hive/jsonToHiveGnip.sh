#!/bin/bash

pull="wisconsin_politics_terms_top_contains"
## Fix float and datetime bugs
hadoop jar double-conversation-1.0-SNAPSHOT.jar org.ahanna.DoubleConversion /user/quevedo/smad/gnip/data/$pull/*/*/*/* gnip-tmp

cat gnippull_rc.sql | sed "s/GNIPPULL/$pull/" > gnippull_rc-tmp.sql

## do sed replacement of GNIPPULL
cat gnip-update.sql | sed "s/GNIPPULL/$pull/" > gnip-update-tmp.sql

## do the Hive insert
hive -f 'gnippull_rc-tmp.sql'
hive -f 'gnip-update-tmp.sql'

#echo "Cleaning up..."
## cleanup, do some checks on whether everything went well
hdfs dfs -rm -r "$pull"_raw
hdfs dfs -rm -r gnip-tmp


