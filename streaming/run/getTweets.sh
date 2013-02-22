hdfs dfs -rm -r output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files $HOME/sandbox/hadoop/streaming/map/tweetMapper.py,$HOME/sandbox/hadoop/streaming/data/keywords.txt,$HOME/sandbox/hadoop/streaming/data/follow-r3.txt \
    -input /user/ahanna/bz2test \
    -output output \
    -mapper "tweetMapper.py -l 1 --hashtag" \
    -reducer org.apache.hadoop.mapred.lib.IdentityReducer \
    -numReduceTasks 1
