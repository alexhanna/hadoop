hdfs dfs -rm -r output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
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
