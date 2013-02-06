hdfs dfs -rm -r /user/ahanna/output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files $HOME/sandbox/hadoop/streaming/map/tweetMapper.py,$HOME/sandbox/hadoop/streaming/data/keywords.txt \
    -input elex2012/elex2012.201210* \
    -output output \
    -mapper "tweetMapper.py -k keywords.txt" \
    -reducer org.apache.hadoop.mapred.lib.IdentityReducer 
