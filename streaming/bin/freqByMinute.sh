hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u5.jar \
    -D stream.map.output.field.separator=\t \
    -D stream.num.map.output.key.fields=1 \
    -D map.output.key.field.separator=\t \
    -D mapred.text.key.partitioner.options=-k1,2 \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/bin/freqByMinute.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/bin/reduceDate.py \
    -input /user/ahanna/elextest \
    -output /user/ahanna/output \
    -mapper filter.py \
    -reducer reduceDate.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \

