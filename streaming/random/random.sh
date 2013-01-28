hadoop dfs -rmr output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u5.jar \
    -D stream.map.output.field.separator="\t" \
    -D stream.num.map.output.key.fields=1 \
    -D mapred.text.key.partitioner.options=-k1,1 \
    -file /home/a/ahanna/sandbox/hadoop/streaming/random/randomMap.py \
    -file /home/a/ahanna/sandbox/hadoop/streaming/random/randomReduce.py \
    -file /home/a/ahanna/sandbox/hadoop/streaming/random/follow-r3.txt \
    -input /user/ahanna/elextest \
    -output output \
    -mapper randomMap.py \
    -reducer randomReduce.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner
