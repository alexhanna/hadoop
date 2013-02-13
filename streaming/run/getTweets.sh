hdfs dfs -rm -r /user/ahanna/output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files $HOME/sandbox/hadoop/streaming/map/tweetMapper.py,$HOME/sandbox/hadoop/streaming/data/keywords.txt,$HOME/sandbox/hadoop/streaming/data/follow-r3.txt \
    -input /user/ahanna/elex2012/elex2012.20120310.json \
    -output output \
    -mapper "tweetMapper.py -l 1 --keywordFile keywords.txt -t low" \
    -reducer org.apache.hadoop.mapred.lib.IdentityReducer \
    -numReduceTasks 1
