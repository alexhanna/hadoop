#!/bin/bash

stage='/scratch.1/streams_gzipd'

year=2016

for month in {7..7}
do
    for day in {1..6}
    do
	STARTTIME=$(date +%s)
	ghfile=$(printf 'gh.%4d%02d%02d' $year $month $day)
	
	echo "#################### $ghfile ####################"
	
	if [ -e $stage/$ghfile.json.gz ]
	then
	    echo "File exists!"
	else
	    echo "File does not exist."
	    continue
	fi

	echo "Uncompressing and uploading to HDFS"
	zcat $stage/$ghfile.json.gz | hdfs dfs -put - gh-stage/$ghfile.json

	## Fix float and datetime bugs
	hadoop jar double-conversation-1.0-SNAPSHOT.jar org.ahanna.DoubleConversion gh-stage gh-tmp

	## do sed replacement of CURRYEAR, CURRMONTH, CURRDAY
	cat update2.sql | sed "s/CURRYEAR/$year/" | sed  "s/CURRMONTH/$month/" | sed "s/CURRDAY/$day/" >| update2-tmp.sql

	## do the Hive insert
	hive -f 'update2-tmp.sql'

	ENDTIME=$(date +%s)
	if [ $? -eq 0 ]
	then
		echo "$ghfile -- Completed in $(($ENDTIME - $STARTTIME)) seconds" >> success3.log
	else
		echo $ghfile >> failure3.log
	fi

	## cleanup, do some checks on whether everything went well
	echo "Cleaning up..."
	hdfs dfs -rm -r gh_raw/year=$year/month=$month/day=$day
	hdfs dfs -rm -r gh-tmp
	hdfs dfs -rm -r gh-stage/*
	# fi
    done
done
