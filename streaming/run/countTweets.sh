## change this depending on the number of fields you are going to use as the key
## for instance, if you are aggregating over dates and keywords, this value will be 2.
NKEY=1

hdfs dfs -rm -r /user/ahanna/output
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D stream.map.output.field.separator="\t" \
    -D stream.num.map.output.key.fields=$NKEY \
    -files $HOME/sandbox/hadoop/streaming/map/countMapper.py,$HOME/sandbox/hadoop/streaming/reduce/nReduce.py,$HOME/sandbox/hadoop/streaming/data/presidential.txt,$HOME/sandbox/hadoop/streaming/data/follow-r3.txt \
    -input jan25 \
    -output output \
    -mapper "countMapper.py -d day" \
    -reducer "nReduce.py $NKEY" \
    -numReduceTasks 1