#!/bin/bash

year=2017
month=3

for day in {23..23}
do
 STARTTIME=$(date +%s)
 ghfile=$(printf 'gh.%4d%02d%02d' $year $month $day)

 echo "#################### $ghfile ####################"
 
 ## upload to stage directly from JSON
# hdfs dfs -put /scratch.1/streams_gzipd/$ghfile.json gh-stage

 ## Fix float and datetime bugs
 hadoop jar /u/a/h/ahanna/sandbox/hadoop/hive/double-conversation-1.0-SNAPSHOT.jar org.ahanna.DoubleConversion $ghfile.json gh-tmp

 ## do sed replacement of CURRYEAR, CURRMONTH, CURRDAY
 cat /u/a/h/ahanna/sandbox/hadoop/hive/update3.sql | sed "s/CURRYEAR/$year/" | sed  "s/CURRMONTH/$month/" | sed "s/CURRDAY/$day/" >| /u/a/h/ahanna/sandbox/hadoop/hive/update3-tmp.sql

 ## do the Hive insert
 hive -f '/u/a/h/ahanna/sandbox/hadoop/hive/update3-tmp.sql'

 ENDTIME=$(date +%s)
 if [ $? -eq 0 ]
 then
     echo "$ghfile -- Completed in $(($ENDTIME - $STARTTIME)) seconds" >> success_rc3.log
 else
     echo $ghfile >> failure3.log
 fi

 ## cleanup, do some checks on whether everything went well
 echo "Cleaning up..."
 hdfs dfs -rm -r gh_raw/year=$year/month=$month/day=$day
 hdfs dfs -rm -r gh-tmp
 hdfs dfs -rm -r gh-stage/*
done
