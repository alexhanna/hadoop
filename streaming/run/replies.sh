hdfs dfs -rm -r output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files $HOME/sandbox/hadoop/streaming/map/replyMapper.py,$HOME/sandbox/hadoop/streaming/reduce/replyReduce.py,$HOME/sandbox/hadoop/streaming/data/follow-all.txt \
    -input elextest \
    -output output \
    -mapper "replyMapper.py -p 1 -s 2 --levelFile follow-all.txt" \
    -reducer replyReduce.py \
    -numReduceTasks 1