hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u5.jar \
    -D stream.map.output.field.separator=\t \
    -D stream.num.map.output.key.fields=2 \
    -D map.output.key.field.separator=\t \
    -D mapred.text.key.partitioner.options=-k1,2 \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/bin/filter.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/bin/reducer.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/bin/latinKeywords.txt \
    -input /user/ahanna/elextest \
    -output /user/ahanna/output \
    -mapper filter.py \
    -reducer reducer.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \

