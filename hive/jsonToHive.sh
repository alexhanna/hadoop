#!/bin/bash

year=2013
month=2

for day in {1..17}
do
    ghfile=$(printf 'gh/gh.%4d%02d%02d*' $year $month $day)
    file_exists=`hdfs dfs -ls $ghfile`

    if [ $? -eq 0 ]
    then
	echo "Converting $ghfile"

	hadoop jar double-conversation-1.0-SNAPSHOT.jar org.ahanna.DoubleConversion $ghfile gh-tmp

	cat update.sql | sed "s/CURRYEAR/$year/" | sed  "s/CURRMONTH/$month/" | sed "s/CURRDAY/$day/" > update-tmp.sql

	hive -f 'update-tmp.sql'

	if [ $? -eq 0 ]
	then
	    echo $ghfile >> success.log
	else
	    echo $ghfile >> failure.log
	fi
	
	hdfs dfs -rm -r gh_raw/year=$year/month=$month/day=$day
	hdfs dfs -rm -r gh-tmp
    fi
done

