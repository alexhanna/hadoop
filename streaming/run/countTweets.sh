## change this depending on the number of fields you are going to use as the key
## for instance, if you are aggregating over dates and keywords, this value will be 2.
NKEY=1

hdfs dfs -rm -r /user/ahanna/output
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D stream.map.output.field.separator="\t" \
    -D stream.num.map.output.key.fields=$NKEY \
    -files $HOME/sandbox/hadoop/streaming/map/countMapper.py,$HOME/sandbox/hadoop/streaming/reduce/nReduce.py \
    -input gh/gh.201202* gh/gh.201203* gh/gh.201204* gh/gh.201205* gh/gh.201206* gh/gh.201207* gh/gh.201208* gh/gh.201209* gh/gh.201210* gh/gh.201211* gh/gh.201212* gh/gh.201301* gh/gh.201202* \
    -output output \
    -mapper "countMapper.py -d day" \
    -reducer "nReduce.py $NKEY" \
    -numReduceTasks 1
