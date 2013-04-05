#jars=/usr/lib/hadoop/lib/avro-1.7.1.cloudera.2.jar,/usr/lib/hive/lib/avro-mapred-1.7.1.cloudera.2.jar
# 	-libjars $jars \

hdfs dfs -rm -r output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files $jars,$HOME/sandbox/hadoop/streaming/map/tweetMapper.py,$HOME/sandbox/hadoop/streaming/data/keywords.txt,$HOME/sandbox/hadoop/streaming/data/follow-r1.txt \
    -input wirecall \
    -output output \
    -mapper "tweetMapper.py --skipRetweets" \
    -reducer org.apache.hadoop.mapred.lib.IdentityReducer \
    -numReduceTasks 1

# org.apache.avro.mapred.AvroAsTextInputFormat
