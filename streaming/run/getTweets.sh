#jars=/usr/lib/hadoop/lib/avro-1.7.1.cloudera.2.jar,/usr/lib/hive/lib/avro-mapred-1.7.1.cloudera.2.jar
# 	-libjars $jars \

hdfs dfs -rm -r output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files $jars,$HOME/sandbox/hadoop/streaming/map/tweetMapper.py,$HOME/sandbox/hadoop/streaming/data/handbook.txt,$HOME/sandbox/hadoop/streaming/data/follow-r1.txt \
    -input elex2012/elex2012.201202* elex2012/elex2012.201203* \
    -output output \
    -mapper "tweetMapper.py --hashtag -k handbook.txt --levelFile follow-r1.txt" \
    -reducer org.apache.hadoop.mapred.lib.IdentityReducer \
    -numReduceTasks 1

# org.apache.avro.mapred.AvroAsTextInputFormat
