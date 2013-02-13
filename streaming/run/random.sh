hdfs dfs -rm -r output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files $HOME/sandbox/hadoop/streaming/map/tweetMapper.py,$HOME/sandbox/hadoop/streaming/reduce/randomReduce.py,$HOME/sandbox/hadoop/streaming/data/follow-r3.txt \
    -input /user/ahanna/elextest \
    -output output \
    -mapper "tweetMapper.py -l 1" \
    -reducer randomReduce.py \
    -numReduceTasks 1
