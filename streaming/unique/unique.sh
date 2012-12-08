hadoop dfs -rmr /user/ahanna/output

hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2-cdh3u5.jar \
    -D stream.map.output.field.separator="\t" \
    -D stream.num.map.output.key.fields=1 \
    -D map.output.key.field.separator="\t" \
    -D mapred.text.key.partitioner.options=-k1,2 \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/unique/getUnique.py \
    -file /home/a/ahanna/sandbox/hadoop-textual-analysis/streaming/unique/reduceUnique.py \
    -input /user/ahanna/elex2010 \
    -output /user/ahanna/output \
    -mapper getUnique.py \
    -reducer reduceUnique.py \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \

