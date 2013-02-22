jars=/usr/lib/hadoop/lib/avro-1.7.1.cloudera.2.jar,/usr/lib/hive/lib/avro-mapred-1.7.1.cloudera.2.jar

hdfs dfs -rm -r output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files $HOME/sandbox/hadoop/streaming/map/tweetMapper.py,$HOME/sandbox/hadoop/streaming/data/keywords.txt,$HOME/sandbox/hadoop/streaming/data/follow-r3.txt \
 	-libjars $jars \
    -input /user/ahanna/avrotest \
    -output output \
    -mapper "tweetMapper.py -l 1 --hashtag" \
    -reducer org.apache.hadoop.mapred.lib.IdentityReducer \
    -inputformat org.apache.avro.mapred.AvroAsTextInputFormat \
    -numReduceTasks 1
